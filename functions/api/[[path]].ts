import type { PagesFunction, R2Bucket, KVNamespace } from '@cloudflare/workers-types';

// --- NEW DATA LOADING --- (Requirement 1 & 2)
// Load poem data from poems.json
import poemsDataRaw from '../../data/poems.json'; // Changed source

// --- Constants ---
const KV_EXPIRATION_TTL_SECONDS = 3600; // 1 hour
// ** Using specified models **
const GEMINI_VISION_MODEL = "gemini-2.0-flash-thinking-exp-01-21";
const GEMINI_TEXT_MODEL = "gemini-2.0-flash-thinking-exp-01-21";
const GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
const MAX_QUESTIONS_PER_GAOKAO_SET = 4; // Target number of questions for "挑戰高考"
const MAX_FEEDBACK_TOKENS = 3000;
const API_RETRY_COUNT = 2;
const API_RETRY_DELAY_MS = 1500;
const TOTAL_SCORE_TARGET = 8; // Target total score for both challenge types

// --- Type Definitions ---

// (Requirement 2) Define type for individual questions extracted from PoemEntry
interface PoemQuestion {
    question: string;
    answer: string;
    year?: number; // Optional year information
    sourceTitle: string; // Link back to the source work
    sourceAuthor?: string;
    sourceDynasty?: string;
    sourceCategory: string;
    sourceOrder: number;
}

// (Requirement 2) Define type for entries in poems.json (raw structure)
interface RawPoemEntry {
    title: string;
    author?: string;
    dynasty?: string;
    category: string;
    order: number;
    paragraphs: string[];
    [key: string]: any; // Allow for question1, answer1, year1 etc.
}

// (Requirement 2) Define type for processed poem entries
interface ProcessedPoemEntry extends Omit<RawPoemEntry, `question${number}` | `reference_answer${number}` | `year${number}`> {
    questions: { question: string; answer: string; year?: number }[];
}


// For a single generated question object used internally and in KV
interface QuestionInfo {
    id: string; // Unique ID for this specific question instance in the set
    question: string; // The question text
    answer: string; // The reference answer
    sourceTitle?: string; // Added field
    sourceAuthor?: string; // Added field
    sourceCategory?: string; // Added field
    sourceOrder?: number; // Added field
}


// For the entire set stored in KV (used only for GaoKao challenge)
interface QuestionSet {
    setId: string; // Unique ID for the entire set
    questions: QuestionInfo[]; // Array of generated questions
    createdAt: number; // Timestamp
}

// Gemini API related types (Keep as they are)
interface GeminiTextPart { text: string; }
interface GeminiImageDataPart { inline_data: { mime_type: string; data: string; }; }
interface GeminiContent { parts: (GeminiTextPart | GeminiImageDataPart)[]; role?: string; }
interface GeminiCandidate { content: GeminiContent; finishReason?: string; index?: number; safetyRatings?: any[]; }
interface GeminiErrorDetail { code: number; message: string; status: string; }
interface GeminiApiResponse {
    candidates?: GeminiCandidate[];
    promptFeedback?: any;
    error?: GeminiErrorDetail;
}


// For scoring results returned to frontend (Keep as they are)
interface SubmissionResult {
    questionIndex: number;
    questionId: string; // Link back to the QuestionInfo id (or chapter question index)
    success: boolean;
    recognizedText: string;
    correctAnswer: string;
    isCorrect: boolean;
    score: number;
    error?: string;
}

// Environment Bindings Interface (Keep as they are)
interface Env {
    IMAGES_BUCKET: R2Bucket;
    GEMINI_API_KEY: string;
    SESSION_KV: KVNamespace;
}

// --- Utility Functions --- (Keep getRandomItems, arrayBufferToBase64, generateUniqueKey, delay)

function getRandomItems<T>(arr: T[], num: number): T[] {
    if (!Array.isArray(arr)) {
        console.error("getRandomItems: input is not an array", arr);
        return [];
    }
    if (num >= arr.length) {
        const shuffledAll = [...arr];
        for (let i = shuffledAll.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [shuffledAll[i], shuffledAll[j]] = [shuffledAll[j], shuffledAll[i]];
        }
        return shuffledAll;
    }
    const shuffled = [...arr];
    for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled.slice(0, num);
}

function arrayBufferToBase64(buffer: ArrayBuffer): string {
    let binary = '';
    const bytes = new Uint8Array(buffer);
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}

function generateUniqueKey(prefix = 'answer', extension = '.png'): string {
     return `${prefix}-${Date.now()}-${crypto.randomUUID()}${extension}`;
}

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// --- Gemini API Call Function (Unchanged) ---
async function callGeminiAPI(apiKey: string, model: string, contents: GeminiContent[], generationConfig?: { maxOutputTokens?: number; temperature?: number; }): Promise<GeminiApiResponse> {
    // ... (Implementation unchanged from the provided snippet, including retry logic)
    const url = `${GEMINI_API_BASE_URL}${model}:generateContent?key=${apiKey}`;
    let lastError: any = null;

    for (let attempt = 0; attempt <= API_RETRY_COUNT; attempt++) {
        console.log(`Calling Gemini API: ${url} (Model: ${model}, Attempt: ${attempt + 1}/${API_RETRY_COUNT + 1})`);
        let response: Response | null = null; // Declare response outside try

        try {
            response = await fetch(url, { // Assign to outer response
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ contents, generationConfig }),
            });
            console.log(`Gemini API response status: ${response.status}`);

            // --- Correction: Clone BEFORE potential JSON parsing ---
            const clonedResponse = response.clone(); // Clone here, accessible in catch
            let responseData: GeminiApiResponse | null = null;
            let errorBodyText: string | null = null;

            try {
                 // --- Correction: Use original 'response' for JSON attempt ---
                 responseData = await response.json() as GeminiApiResponse;

                 if (responseData?.error) {
                     console.error(`Gemini API Error in JSON response body:`, responseData.error);
                     const errorDetail = responseData.error;
                     throw new Error(`AI API Error ${errorDetail.code} (${errorDetail.status}): ${errorDetail.message}`);
                 }
            } catch (jsonError: any) {
                // --- Correction: Use 'clonedResponse' to read text if JSON failed ---
                console.warn("Gemini API response was not valid JSON or JSON error parsing failed. Reading as text.", jsonError.message);
                try {
                     errorBodyText = await clonedResponse.text(); // Use the clone
                     console.error(`Gemini API Error Response Body (Text):`, errorBodyText);
                 } catch (textError: any) {
                     console.error("Failed to read Gemini API response body as text:", textError);
                     errorBodyText = "[Failed to read error body]";
                 }

                // If the original response status was not ok, throw based on status
                // --- Correction: Check outer 'response' status ---
                if (!response.ok) {
                     throw new Error(`AI API Error (${response.status}): ${response.statusText}. Body: ${errorBodyText}`);
                }
                console.error("Gemini API returned ok status but invalid JSON response:", errorBodyText);
                throw new Error("AI API returned unexpected response format (non-JSON or malformed JSON).");
            }

            // If response is OK and we have valid JSON data
            // --- Correction: Check outer 'response' status ---
            if (response.ok && responseData) {
                // Check for potential non-error cases where candidates might be missing (e.g., safety filters)
                if (!responseData.candidates && !responseData.error) {
                    console.warn("Gemini API call successful but response data missing candidates without explicit error.", responseData);
                }
                return responseData; // Success
            }

            // If response was not OK, re-throw (should have been caught above, but defense in depth)
            // --- Correction: Check outer 'response' status ---
            if (!response.ok) {
                 throw new Error(`AI API Error (${response.status}): ${response.statusText}. Body: ${errorBodyText ?? JSON.stringify(responseData)}`);
            }

            // Fallback case (should be rare)
            console.warn("Gemini API call status was ok but data is invalid:", responseData);
            return responseData ?? {};

        } catch (error: any) {
            lastError = error;
            console.error(`Gemini API Call Attempt ${attempt + 1} failed:`, error.message);

            // --- Correction: Use outer 'response' status for retry logic if available ---
            const statusCode = response?.status; // Get status code if response object exists
            const errorMessage = error.message.toLowerCase();
            const isOverloaded = errorMessage.includes("overloaded") || errorMessage.includes("resource has been exhausted") || errorMessage.includes("try again later");
            // Check status code OR message for retryable conditions
            const isRetryableStatusCode = statusCode === 503 || statusCode === 500 || statusCode === 429;


            if ((isOverloaded || isRetryableStatusCode) && attempt < API_RETRY_COUNT) {
                const delayTime = API_RETRY_DELAY_MS * Math.pow(2, attempt);
                console.log(`Retryable error detected (Status: ${statusCode}, Message: ${error.message}). Retrying in ${delayTime}ms...`);
                await delay(delayTime);
                continue;
            }

            console.error("Non-retryable error or retries exhausted. Throwing last error.");
            throw lastError;
        }
    }
    // Safeguard
    throw lastError || new Error("Gemini API call failed after all retries.");
}


// --- **TYPE GUARD for poems.json data** --- (Requirement 2)
function isValidPoemsData(data: any): data is RawPoemEntry[] {
    console.log("isValidPoemsData: Function called");

    if (!Array.isArray(data)) {
        console.error("isValidPoemsData: Input is not an array.", data);
        throw new Error("Data is not an array.");
    }
    if (data.length === 0) {
        console.error("isValidPoemsData: Array is empty.");
        throw new Error("Data array is empty.");
    }
    const sample = data[0];
    // Check for core fields and at least one question pair
    const isValid = typeof sample?.title === 'string' &&
                    typeof sample?.category === 'string' &&
                    typeof sample?.order === 'number' &&
                    Array.isArray(sample?.paragraphs) &&
                    typeof sample?.question1 === 'string' && // Check for at least the first question
                    typeof sample?.reference_answer1 === 'string';

    if (!isValid) {
        const errorMessage = "First item in data array is invalid or missing required fields (title, category, order, paragraphs, question1, reference_answer1). Structure is incorrect.";
        console.error("isValidPoemsData: First item structure is invalid.", sample);
        console.error("Types:", {
            title: typeof sample?.title,
            category: typeof sample?.category,
            order: typeof sample?.order,
            paragraphs: Array.isArray(sample?.paragraphs),
            question1: typeof sample?.question1,
            reference_answer1: typeof sample?.reference_answer1
        });
        throw new Error(errorMessage);
    }
    return isValid;
}

// --- (Requirement 2) Helper to transform raw poem data ---
function transformRawPoemsData(rawData: RawPoemEntry[]): ProcessedPoemEntry[] {
    return rawData.map(entry => {
        const questions: { question: string; answer: string; year?: number }[] = [];
        let i = 1;
        while (entry[`question${i}`] && entry[`reference_answer${i}`]) {
            questions.push({
                question: entry[`question${i}`],
                answer: entry[`reference_answer${i}`],
                year: entry[`year${i}`] ? Number(entry[`year${i}`]) : undefined
            });
            i++;
        }
        // Create a new object without the raw questionX etc fields
        const { title, author, dynasty, category, order, paragraphs } = entry;
        return { title, author, dynasty, category, order, paragraphs, questions };
    });
}

// Global variable to hold processed poem data after validation
let processedPoemEntries: ProcessedPoemEntry[] = [];

// --- Main Request Handler ---
export const onRequest: PagesFunction<Env> = async (context) => {
    console.log("onRequest: API request received");
    const { request, env, params } = context;
    const url = new URL(request.url);
    const apiPath = (params.path as string[] || []).join('/');
    // Extract query parameters for chapter selection
    const chapterOrderParam = url.searchParams.get('order');

    const baseHeaders = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    };

    if (request.method === 'OPTIONS') {
        return new Response(null, { headers: baseHeaders });
    }

    console.log(`[${new Date().toISOString()}] Request: ${request.method} /api/${apiPath}${url.search}`);

    if (!env.IMAGES_BUCKET || !env.GEMINI_API_KEY || !env.SESSION_KV) {
         console.error("FATAL: Server configuration error: Missing required Cloudflare bindings (R2, KV) or secrets (GEMINI_API_KEY).");
         return new Response(JSON.stringify({ error: "Server configuration error. Please contact administrator." }), { status: 500, headers: baseHeaders });
    }

    // --- **DATA VALIDATION & PROCESSING (ONCE)** --- (Requirement 2)
    // Only process if not already done
    if (processedPoemEntries.length === 0) {
        if (isValidPoemsData(poemsDataRaw)) {
            processedPoemEntries = transformRawPoemsData(poemsDataRaw);
            console.log(`Successfully validated and processed poems.json data. Found ${processedPoemEntries.length} entries.`);
        } else {
            console.error("FATAL: poems.json data failed validation. Check the file structure and content.");
            return new Response(JSON.stringify({ error: "內部題庫數據格式錯誤，無法提供服務。" }), { status: 500, headers: baseHeaders });
        }
    }

    try {
        // --- API Routing ---

        if (apiPath === 'hello' && request.method === 'GET') {
            const dataInfo = {
                message: "Backend is running.",
                status: "OK",
                timestamp: new Date().toISOString(),
                poemEntriesLoaded: processedPoemEntries.length, // Report count from processed data
                modelsUsed: { vision: GEMINI_VISION_MODEL, text: GEMINI_TEXT_MODEL },
                maxFeedbackTokens: MAX_FEEDBACK_TOKENS
            };
            return new Response(JSON.stringify(dataInfo), { headers: baseHeaders });
        }

        // --- Route for "挑戰高考" --- (Requirement 5)
        if (apiPath === 'start_gaokao_set' && request.method === 'GET') {
            console.log("Processing /api/start_gaokao_set request using processed poems.json data");

            // Flatten all questions from all entries
            const allIndividualQuestions: PoemQuestion[] = [];
            processedPoemEntries.forEach(entry => {
                entry.questions.forEach(q => {
                    allIndividualQuestions.push({
                        question: q.question,
                        answer: q.answer,
                        year: q.year,
                        sourceTitle: entry.title,
                        sourceAuthor: entry.author,
                        sourceDynasty: entry.dynasty,
                        sourceCategory: entry.category,
                        sourceOrder: entry.order
                    });
                });
            });

            if (allIndividualQuestions.length < MAX_QUESTIONS_PER_GAOKAO_SET) {
                throw new Error(`題庫中的總題目數量 (${allIndividualQuestions.length}) 不足 ${MAX_QUESTIONS_PER_GAOKAO_SET} 道，無法出題。`);
            }

            const setId = crypto.randomUUID();

            // Select random individual questions from the flattened list
            const selectedQuestions = getRandomItems(allIndividualQuestions, MAX_QUESTIONS_PER_GAOKAO_SET);

            // Map the selected questions to the QuestionInfo format for KV storage
            const generatedQuestions: QuestionInfo[] = selectedQuestions.map((poemQ) => {
                return {
                    id: crypto.randomUUID(), // Unique ID for this instance
                    question: poemQ.question,
                    answer: poemQ.answer,
                    sourceTitle: poemQ.sourceTitle,
                    sourceAuthor: poemQ.sourceAuthor,
                    sourceCategory: poemQ.sourceCategory, // Use category instead of type (Requirement 3 implicitly handled here)
                    sourceOrder: poemQ.sourceOrder
                };
            });

            console.log(`Selected ${generatedQuestions.length} questions from all poems for GaoKao setId: ${setId}`);

             // Store the generated set in KV
             const newSet: QuestionSet = { setId, questions: generatedQuestions, createdAt: Date.now() };
             try {
                 await env.SESSION_KV.put(setId, JSON.stringify(newSet), { expirationTtl: KV_EXPIRATION_TTL_SECONDS });
                 console.log(`Stored new GaoKao question set in KV with setId: ${setId} (${generatedQuestions.length} questions)`);
             } catch (kvError: any) {
                 console.error(`KV put error for GaoKao setId ${setId}:`, kvError);
                 throw new Error(`無法保存生成的題組信息: ${kvError.message}`);
             }

             // Prepare response for the frontend (without answers)
             const questionsForFrontend = newSet.questions.map(({ answer, ...rest }: QuestionInfo) => rest);
             return new Response(JSON.stringify({ setId: newSet.setId, questions: questionsForFrontend }), { headers: baseHeaders });

        } // End /api/start_gaokao_set

        // --- Route to get chapter list for "選篇挑戰" --- (Requirement 6)
        if (apiPath === 'get_chapters' && request.method === 'GET') {
            console.log("Processing /api/get_chapters request");
            const chapterList = processedPoemEntries
                .map(entry => ({ order: entry.order, title: entry.title }))
                .sort((a, b) => a.order - b.order); // Sort by order

            return new Response(JSON.stringify({ chapters: chapterList }), { headers: baseHeaders });
        }

        // --- Route to get questions for a specific chapter --- (Requirement 7)
        if (apiPath === 'get_chapter_questions' && request.method === 'GET') {
            console.log(`Processing /api/get_chapter_questions request for order: ${chapterOrderParam}`);
            if (!chapterOrderParam) {
                 return new Response(JSON.stringify({ error: '請求無效：缺少篇目順序號 (order)。' }), { status: 400, headers: baseHeaders });
            }
            const order = parseInt(chapterOrderParam, 10);
            if (isNaN(order)) {
                 return new Response(JSON.stringify({ error: '請求無效：篇目順序號 (order) 必須是數字。' }), { status: 400, headers: baseHeaders });
            }

            const chapterEntry = processedPoemEntries.find(entry => entry.order === order);

            if (!chapterEntry) {
                return new Response(JSON.stringify({ error: `未找到順序號為 ${order} 的篇目。` }), { status: 404, headers: baseHeaders });
            }

            // Map chapter questions to QuestionInfo format (without answers) for frontend
            const questionsForFrontend: Omit<QuestionInfo, 'answer'>[] = chapterEntry.questions.map((q, index) => ({
                id: `chapter-${chapterEntry.order}-q${index}`, // Create a predictable ID for chapter questions
                question: q.question,
                sourceTitle: chapterEntry.title,
                sourceAuthor: chapterEntry.author,
                sourceCategory: chapterEntry.category,
                sourceOrder: chapterEntry.order
            }));

            console.log(`Found ${questionsForFrontend.length} questions for chapter order ${order} (${chapterEntry.title})`);
            return new Response(JSON.stringify({
                chapterOrder: chapterEntry.order,
                chapterTitle: chapterEntry.title,
                questions: questionsForFrontend
            }), { headers: baseHeaders });
        }


        // --- '/submit' route (Handles both GaoKao and Chapter challenges) ---
        if (apiPath === 'submit' && request.method === 'POST') {
            console.log("Processing /api/submit request");

            // --- Request Parsing and Validation ---
            const formData = await request.formData();
            const setIdValue = formData.get('setId'); // For GaoKao challenge
            const chapterOrderValue = formData.get('chapterOrder'); // For Chapter challenge
            const imageValue = formData.get('handwritingImage');
            let imageFile: File;

            let challengeType: 'gaokao' | 'chapter' | 'unknown' = 'unknown';
            let challengeIdentifier: string = '';

            // Validate challenge identifier (either setId or chapterOrder must be present)
            if (typeof setIdValue === 'string' && setIdValue) {
                challengeType = 'gaokao';
                challengeIdentifier = setIdValue;
                console.log(`Submit request identified as GaoKao challenge (setId: ${challengeIdentifier})`);
            } else if (typeof chapterOrderValue === 'string' && chapterOrderValue) {
                challengeType = 'chapter';
                challengeIdentifier = chapterOrderValue;
                console.log(`Submit request identified as Chapter challenge (chapterOrder: ${challengeIdentifier})`);
            } else {
                 console.error("Invalid submit request: Missing or invalid setId or chapterOrder.", { setIdValue, chapterOrderValue });
                 return new Response(JSON.stringify({ error: '請求無效：缺少有效的挑戰標識 (題組 ID 或篇目順序號)。' }), { status: 400, headers: baseHeaders });
            }

            // Validate imageValue (same as before)
            if (!imageValue || typeof imageValue === 'string' || (imageValue as File).size === 0) {
                console.error("Invalid submit request: Missing or invalid image.", { challengeType, challengeIdentifier });
                const errorField = !imageValue ? '未上傳圖片' : (typeof imageValue === 'string' ? '數據格式不正確（應為文件）' : '圖片文件大小為 0');
                return new Response(JSON.stringify({ error: `請求無效：${errorField}。` }), { status: 400, headers: baseHeaders });
            }
            imageFile = imageValue as File;
            console.log(`Validation passed for ${challengeType} challenge ${challengeIdentifier}. Image: ${imageFile.name}, Size: ${imageFile.size}, Type: ${imageFile.type}`);


            // --- Retrieve Correct Answers and Question Info ---
            let questionsToScore: QuestionInfo[] = [];
            let expectedQuestionCount = 0;

            if (challengeType === 'gaokao') {
                let questionSet: QuestionSet | null = null;
                try {
                    questionSet = await env.SESSION_KV.get<QuestionSet>(challengeIdentifier, 'json');
                } catch (kvError: any) {
                    console.error(`KV get error for GaoKao setId ${challengeIdentifier}:`, kvError);
                    return new Response(JSON.stringify({ error: "無法獲取“挑戰高考”題組信息，會話可能已過期或ID無效，請重新開始。" }), { status: 404, headers: baseHeaders });
                }
                if (!questionSet || !questionSet.questions || questionSet.questions.length === 0 ) {
                    console.error(`Invalid or missing/empty GaoKao question set data in KV for setId ${challengeIdentifier}`, questionSet);
                    return new Response(JSON.stringify({ error: "無效的“挑戰高考”題組信息，請重新開始。" }), { status: 400, headers: baseHeaders });
                }
                questionsToScore = questionSet.questions; // Already in QuestionInfo format
                expectedQuestionCount = questionsToScore.length;
                console.log(`Found ${expectedQuestionCount} GaoKao questions for setId ${challengeIdentifier}.`);
            }
            else if (challengeType === 'chapter') {
                const order = parseInt(challengeIdentifier, 10);
                 if (isNaN(order)) {
                    return new Response(JSON.stringify({ error: '請求無效：篇目順序號 (chapterOrder) 必須是數字。' }), { status: 400, headers: baseHeaders });
                 }
                const chapterEntry = processedPoemEntries.find(entry => entry.order === order);
                 if (!chapterEntry) {
                     return new Response(JSON.stringify({ error: `提交失敗：未找到順序號為 ${order} 的篇目數據。` }), { status: 404, headers: baseHeaders });
                 }
                 // Map chapter questions to QuestionInfo format, including answers
                 questionsToScore = chapterEntry.questions.map((q, index) => ({
                     id: `chapter-${chapterEntry.order}-q${index}`, // Use the predictable ID
                     question: q.question,
                     answer: q.answer, // Include the answer!
                     sourceTitle: chapterEntry.title,
                     sourceAuthor: chapterEntry.author,
                     sourceCategory: chapterEntry.category,
                     sourceOrder: chapterEntry.order
                 }));
                 expectedQuestionCount = questionsToScore.length;
                 console.log(`Found ${expectedQuestionCount} questions for Chapter ${challengeIdentifier} (${chapterEntry.title}).`);
            }

            if (expectedQuestionCount === 0) {
                 console.error(`Logic error: Expected question count is zero for ${challengeType} challenge ${challengeIdentifier}`);
                 return new Response(JSON.stringify({ error: '內部錯誤：未能加載到任何題目信息。' }), { status: 500, headers: baseHeaders });
            }

            const correctAnswers = questionsToScore.map((q: QuestionInfo) => q.answer);
            const questionIds = questionsToScore.map((q: QuestionInfo) => q.id); // Use the generated IDs
            console.log(`Retrieved correct answers for ${challengeType} challenge ${challengeIdentifier}`);

            // --- Store Image to R2 (same as before) ---
            const imageBuffer = await imageFile.arrayBuffer();
            const r2Key = generateUniqueKey(`${challengeType}-${challengeIdentifier}-answer`, `.${imageFile.type.split('/')[1] || 'png'}`);
            try {
                 await env.IMAGES_BUCKET.put(r2Key, imageBuffer, { httpMetadata: { contentType: imageFile.type }});
                 console.log(`Stored image in R2 with key: ${r2Key} for ${challengeType} challenge ${challengeIdentifier}`);
            } catch (r2Error: any) {
                 console.error(`R2 put error for key ${r2Key}:`, r2Error);
                 return new Response(JSON.stringify({ error: `圖片存儲失敗: ${r2Error.message || 'Unknown R2 error'}` }), { status: 500, headers: baseHeaders });
            }


            // --- Call Gemini Vision for OCR ---
            const base64ImageData = arrayBufferToBase64(imageBuffer);
            const ocrStartTime = Date.now();
            let recognizedTextCombined = '';
            let ocrError: string | null = null;
            let splitAnswers: string[] = [];

            // Dynamically create the prompt based on expected question count
            const ocrPromptText = `这是一张包含${expectedQuestionCount}个手写简体中文答案的图片，按从上到下的顺序排列。请准确识别每个答案，并只用换行符（\\n）分隔返回${expectedQuestionCount}个结果。不要添加任何其他文字、解释、编号或格式。如果某个答案无法识别，请在那一行输出 "[無法識別]"。`;
            console.log(`Using OCR prompt for ${expectedQuestionCount} answers.`);

            const ocrContents: GeminiContent[] = [{
                parts: [
                    { "text": ocrPromptText },
                    { "inline_data": { "mime_type": imageFile.type || "image/png", "data": base64ImageData } }
                ]
            }];

            // ... (Rest of OCR call, error handling, splitting logic is the same as before, using expectedQuestionCount)
            try {
                const geminiResult = await callGeminiAPI(env.GEMINI_API_KEY, GEMINI_VISION_MODEL, ocrContents, { maxOutputTokens: 800 + (expectedQuestionCount * 50), temperature: 0.1 }); // Slightly increase token allowance based on count
                const ocrDuration = Date.now() - ocrStartTime;
                console.log(`Gemini OCR completed for ${challengeType} challenge ${challengeIdentifier} in ${ocrDuration}ms.`);

                const candidate = geminiResult.candidates?.[0];
                const part = candidate?.content?.parts?.[0];
                const ocrFinishReason = candidate?.finishReason;
                console.log(`Gemini OCR candidate finish reason: ${ocrFinishReason} for ${challengeType} challenge ${challengeIdentifier}`);
                if (ocrFinishReason && ocrFinishReason !== "STOP") {
                    console.warn(`OCR process potentially incomplete. Finish Reason: ${ocrFinishReason}`);
                    ocrError = ocrError ? `${ocrError}. ` : '';
                    ocrError += `AI處理可能未完成 (${ocrFinishReason})。`;
                }

                if (part && 'text' in part) {
                    recognizedTextCombined = part.text.trim();
                } else if (geminiResult.error) {
                     ocrError = `AI OCR 服務錯誤: ${geminiResult.error.message}`;
                     console.error(`OCR API Error from structure for ${challengeType} challenge ${challengeIdentifier}:`, geminiResult.error);
                } else {
                    ocrError = "AI OCR 返回了非預期的響應格式 (無文本部分)。";
                    console.warn(`OCR Result format issue for ${challengeType} challenge ${challengeIdentifier}. Full Response:`, JSON.stringify(geminiResult));
                }

                if (!ocrError && !recognizedTextCombined) {
                     if (ocrFinishReason === "SAFETY") ocrError = "AI OCR 因安全設置拒絕處理圖片內容。";
                     else if (ocrFinishReason === "RECITATION") ocrError = "AI OCR 因檢測到引用內容而停止。";
                     else if (ocrFinishReason === "MAX_TOKENS") ocrError = "AI OCR 處理超時或輸出長度受限。";
                     else ocrError = "AI OCR 未能識別出任何文本內容。";
                    console.warn(`OCR Result empty for ${challengeType} challenge ${challengeIdentifier}. Finish Reason: ${ocrFinishReason}`);
                } else if (!ocrError) {
                    console.log(`Raw OCR result for ${challengeType} challenge ${challengeIdentifier}: "${recognizedTextCombined.replace(/\n/g, '\\n')}"`);
                    splitAnswers = recognizedTextCombined.split('\n').map(s => s.trim());

                    if (splitAnswers.length !== expectedQuestionCount) {
                        console.warn(`OCR split count mismatch for ${challengeType} challenge ${challengeIdentifier}: expected ${expectedQuestionCount}, got ${splitAnswers.length}. Raw: "${recognizedTextCombined}"`);
                        ocrError = `AI OCR 未能準確分割出 ${expectedQuestionCount} 個答案 (找到了 ${splitAnswers.length} 個)。答案可能擠在一起或部分無法識別。`;
                        while (splitAnswers.length < expectedQuestionCount) splitAnswers.push("[答案缺失]");
                        if (splitAnswers.length > expectedQuestionCount) splitAnswers = splitAnswers.slice(0, expectedQuestionCount);
                    } else {
                        console.log(`Successfully split OCR into ${splitAnswers.length} answers for ${challengeType} challenge ${challengeIdentifier}.`);
                    }
                }
            } catch (err: any) {
                 console.error(`Gemini OCR API call failed for ${challengeType} challenge ${challengeIdentifier}:`, err);
                 ocrError = `AI OCR 識別服務調用失敗: ${err.message}`;
                 splitAnswers = Array(expectedQuestionCount).fill(`[OCR調用失敗]`);
            }


            // --- Scoring ---
            const results: SubmissionResult[] = [];
            let totalScore = 0;
            // Calculate points per question to reach the TOTAL_SCORE_TARGET
            const pointsPerQuestion = expectedQuestionCount > 0 ? (TOTAL_SCORE_TARGET / expectedQuestionCount) : 0;

            function removePunctuation(text: string): string {
                if (typeof text !== 'string') return text;
                // Keep basic Chinese punctuation often used in answers if needed, otherwise remove all
                 // return text.replace(/[^\p{L}\p{N}]/gu, ''); // Keeps letters and numbers only
                 return text.replace(/[\p{P}\p{S}\p{Z}]+/gu, ''); // Removes punctuation, symbols, separators
            }

            for (let i = 0; i < expectedQuestionCount; i++) {
                const recognized = splitAnswers[i] !== undefined ? splitAnswers[i] : "[答案缺失]";
                const correct = correctAnswers[i];
                const questionId = questionIds[i]; // Use the previously mapped ID
                let isCorrect = false;
                let score = 0;
                let success = !recognized.startsWith("[") || recognized === "[無法識別]";
                let itemError: string | undefined = undefined;

                if (recognized === "[OCR調用失敗]" || recognized === "[答案提取失敗]" || recognized === "[答案缺失]") {
                    itemError = recognized.substring(1, recognized.length - 1);
                    success = false;
                } else if (recognized === "[無法識別]") {
                     itemError = "AI 無法識別此答案";
                     success = false;
                 }

                if (success && correct !== undefined) {
                    const cleanedRecognized = removePunctuation(recognized);
                    const cleanedCorrect = removePunctuation(correct);
                    isCorrect = cleanedRecognized === cleanedCorrect && cleanedRecognized !== "";

                     if (!isCorrect && cleanedRecognized === "" && recognized !== "") itemError = "識別結果僅包含標點或空格";
                     else if (recognized.trim() === "" && !itemError) itemError = "未作答或未識別到內容";

                    score = isCorrect ? pointsPerQuestion : 0;
                } else {
                    isCorrect = false; score = 0;
                    if (correct === undefined) {
                         itemError = itemError ? `${itemError}; 標準答案缺失` : "標準答案缺失";
                         success = false;
                    }
                 }

                results.push({
                     questionIndex: i,
                     questionId: questionId, // Use the stored question ID
                     success: success,
                     recognizedText: recognized,
                     correctAnswer: correct || "[標準答案缺失]",
                     isCorrect: isCorrect,
                     score: score, // Use calculated score
                     error: itemError
                });
                totalScore += score;
            }
            // Round final score to avoid floating point issues
            totalScore = Math.round(totalScore * 10) / 10;
            console.log(`Scoring complete for ${challengeType} challenge ${challengeIdentifier}. Total score: ${totalScore} / ${TOTAL_SCORE_TARGET}`);

            // --- Generate Feedback using AI ---
            let feedback = "";
            const feedbackStartTime = Date.now();
            let feedbackErrorMsg: string | null = null;
            let feedbackFinishReason: string | null = null;

            // --- Rank/Badge Logic (Only applies to GaoKao challenge) ---
            let currentRank = 0; // Default rank
            let badge = "";
            const rankKey = `user-rank-${challengeIdentifier}`; // Use setId for rank tracking

            if (challengeType === 'gaokao') {
                // Get Current Rank from KV
                try {
                    const storedRank = await env.SESSION_KV.get(rankKey, 'text');
                    if (storedRank) {
                        currentRank = parseInt(storedRank, 10) || 0;
                    }
                } catch (kvRankError) {
                    console.error("Failed to get rank from KV:", kvRankError);
                }
            }


            // --- Generate Feedback Text ---
            // Same prompt logic, adapted score context
            const scoreTarget = TOTAL_SCORE_TARGET;
             if (totalScore === scoreTarget) {
                 feedback = `太棒了！滿分 ${scoreTarget} 分！簡直是默寫的神！繼續保持！`;
                 feedbackErrorMsg = null;

                 // --- Rank Increase and Badge (Only for GaoKao) ---
                 if (challengeType === 'gaokao') {
                     currentRank++;
                     badge = `${convertToChineseRank(currentRank)}階`;
                     if (currentRank === 1) badge = `初窺門徑`;
                     if (currentRank >= 7) badge = `巔峰七階`;
                }

            } else {
                const incorrectResults = results.filter(r => !r.isCorrect);
                const errorDetails = incorrectResults
                    .map((r: SubmissionResult) => {
                        let reason = r.error ? `(原因: ${r.error})` : '(內容錯誤)';
                        if (r.recognizedText === '[無法識別]') reason = '(字跡無法識別)';
                        else if (r.recognizedText === '[答案缺失]') reason = '(未找到對應答案)';
                        else if (r.recognizedText === '[答案提取失敗]') reason = '(答案提取過程失敗)';
                        else if (r.recognizedText === '[OCR調用失敗]') reason = '(圖片識別過程失敗)';
                        else if (removePunctuation(r.recognizedText) === '') reason = '(未作答或僅有標點)';
                        // Use questionIndex which is 0-based
                        return `第 ${r.questionIndex + 1} 題 ${reason}:\n  你的答案: "${r.recognizedText}"\n  正確答案: "${r.correctAnswer}"`;
                    })
                    .join('\n\n');

                const feedbackPrompt = `你扮演一位非常溫和、有耐心的高考語文老師，你的目標是幫助學生從錯誤中學習，建立信心。學生這次默寫挑戰（滿分${scoreTarget}分）沒有拿到滿分，得分 ${totalScore.toFixed(1)}，失分 ${(scoreTarget - totalScore).toFixed(1)}。你需要用充滿鼓勵和關懷的語氣來進行點評。

務必包含以下要素：
1.  溫和地指出得分和失分，強調進步的空間和潛力。
2.  針對學生的具體錯誤（如果有的話），進行溫暖的分析，並提供具體、可操作的改進建議（例如針對字跡、記憶方法、理解深度等）。
3.  多使用鼓勵性、支持性的詞語。
4.  結尾表達對學生未來進步的真誠期待和信心。
5.  字數要求：至少150字以上，確保內容充滿關懷和指導性。

錯誤詳情:
${errorDetails || "（雖然沒有檢測到具體的單題錯誤細節，但整體仍有提升空間，繼續努力！）"}
${ocrError ? `\n圖片識別提示: ${ocrError}` : ''}

現在，請開始用溫和且鼓勵的語氣進行點評吧！`;

                // ... (Rest of feedback generation call and processing is the same)
                 try {
                    console.log(`Generating Gentle AI feedback for ${challengeType} challenge ${challengeIdentifier}...`);
                    const feedbackContents: GeminiContent[] = [{ parts: [{ text: feedbackPrompt }] }];
                    const feedbackResult = await callGeminiAPI(
                        env.GEMINI_API_KEY,
                        GEMINI_TEXT_MODEL,
                        feedbackContents,
                        { maxOutputTokens: MAX_FEEDBACK_TOKENS, temperature: 0.8 }
                    );

                    let generatedText: string | null = null;
                    let extractionFailureReason = "Unknown structure issue";
                    const candidate = feedbackResult.candidates?.[0];
                    feedbackFinishReason = candidate?.finishReason ?? null;
                    console.log(`Gemini feedback generation candidate finish reason: ${feedbackFinishReason} for ${challengeType} challenge ${challengeIdentifier}`);

                    if (candidate?.content?.parts?.[0] && 'text' in candidate.content.parts[0]) {
                        const trimmedText = candidate.content.parts[0].text.trim();
                        if (trimmedText.length > 0) { generatedText = trimmedText; extractionFailureReason = ""; }
                        else { extractionFailureReason = "Extracted text is empty."; console.warn(`AI feedback empty for ${challengeType} challenge ${challengeIdentifier}. Finish: ${feedbackFinishReason}`); }
                    } else if (feedbackResult.error) {
                         extractionFailureReason = `API Error: ${feedbackResult.error.message}`; console.error(`AI feedback API error for ${challengeType} challenge ${challengeIdentifier}:`, feedbackResult.error);
                    } else {
                         if (!feedbackResult.candidates?.length) extractionFailureReason = "No candidates.";
                         else if (!candidate?.content?.parts?.length) extractionFailureReason = "No parts.";
                         else extractionFailureReason = "First part not text.";
                         console.warn(`AI feedback extraction: ${extractionFailureReason} for ${challengeType} challenge ${challengeIdentifier}. Resp:`, JSON.stringify(feedbackResult));
                    }

                    if (feedbackFinishReason && feedbackFinishReason !== "STOP" && generatedText !== null) {
                        let reasonWarning = "";
                        if (feedbackFinishReason === "MAX_TOKENS") reasonWarning = "回覆可能因長度限制被截斷。";
                        else if (feedbackFinishReason === "SAFETY") reasonWarning = "回覆可能因安全設置被部分過濾。";
                        else if (feedbackFinishReason === "RECITATION") reasonWarning = "回覆可能因檢測到引用內容而提前終止。";
                        else reasonWarning = `回覆處理因 (${feedbackFinishReason}) 而結束。`;
                        feedback = `${generatedText}\n\n[系統提示: ${reasonWarning}]`;
                        feedbackErrorMsg = reasonWarning;
                    } else if (generatedText !== null) {
                         feedback = generatedText; feedbackErrorMsg = null;
                         console.log(`AI feedback generated successfully for ${challengeType} challenge ${challengeIdentifier}.`);
                     } else {
                        console.error(`Failed to extract AI feedback for ${challengeType} challenge ${challengeIdentifier}. Reason: ${extractionFailureReason}. Finish: ${feedbackFinishReason}. Using fallback.`);
                        console.error("Full Gemini Response causing fallback:", JSON.stringify(feedbackResult, null, 2));
                        let fallbackReason = extractionFailureReason;
                         if (!fallbackReason.toLowerCase().includes("api error") && feedbackFinishReason && feedbackFinishReason !== "STOP") {
                             fallbackReason += ` (處理結束原因: ${feedbackFinishReason})`;
                         }
                        feedbackErrorMsg = `AI 反饋生成成功，但內容提取失敗 (${fallbackReason})。`;
                        feedback = `得分 ${totalScore.toFixed(1)}，失分 ${(scoreTarget - totalScore).toFixed(1)}。這次表現還有進步空間哦。看看下面的錯誤細節，下次加油！\n錯誤詳情:\n${errorDetails || "（未能生成詳細的錯誤分析）"}`;
                    }

                } catch (feedbackError: any) {
                     console.error(`Gemini feedback generation failed for ${challengeType} challenge ${challengeIdentifier}:`, feedbackError);
                     feedbackErrorMsg = `AI 反饋生成服務調用失敗: ${feedbackError.message}`;
                     feedback = `得分 ${totalScore.toFixed(1)}，失分 ${(scoreTarget - totalScore).toFixed(1)}。這次表現還有進步空間哦。看看下面的錯誤細節，下次加油！\n錯誤詳情:\n${errorDetails || "（未能生成詳細的錯誤分析）"}`;
                }

                 // --- Rank Decrease (Only for GaoKao) ---
                 if (challengeType === 'gaokao') {
                     if (currentRank > 0) currentRank--;
                     badge = currentRank > 0 ? `${convertToChineseRank(currentRank)}階` : "初窺門徑";
                     if (currentRank >= 7) badge = `巔峰七階`;
                 }
            }

            // --- Store Updated Rank (Only for GaoKao) ---
            if (challengeType === 'gaokao') {
                try {
                    await env.SESSION_KV.put(rankKey, String(currentRank), { expirationTtl: KV_EXPIRATION_TTL_SECONDS });
                    console.log(`GaoKao Rank updated to ${currentRank} for setId ${challengeIdentifier}`);
                } catch (kvPutRankError) {
                    console.error("Failed to put rank to KV:", kvPutRankError);
                }
            }

            // --- Prepare Final Response ---
            let finalMessage = "評分完成。";
            // ... (Final message construction based on ocrError/feedbackErrorMsg remains the same)
            if (ocrError && feedbackErrorMsg) finalMessage = "評分完成，但圖片識別和 AI 反饋生成均遇到問題。";
            else if (ocrError) finalMessage = "評分完成，但圖片識別過程遇到問題。";
            else if (feedbackErrorMsg) {
                 let feedbackIssueDetail = feedbackErrorMsg;
                 if (feedbackFinishReason && feedbackFinishReason !== "STOP" && !feedbackErrorMsg.includes(feedbackFinishReason)) {
                     feedbackIssueDetail += ` (原因: ${feedbackFinishReason})`;
                 }
                 finalMessage = `評分完成，但 AI 反饋生成過程遇到問題: ${feedbackIssueDetail}`;
            }

            const responseData: any = { // Use 'any' temporarily for flexibility
                message: finalMessage,
                totalScore: totalScore,
                scoreTarget: scoreTarget, // Send the target score back
                results: results,
                feedback: feedback,
                r2Key: r2Key,
                ocrIssue: ocrError,
                feedbackIssue: feedbackErrorMsg,
            };

            // Add rank/badge only if it was a GaoKao challenge
            if (challengeType === 'gaokao') {
                responseData.rank = currentRank;
                responseData.badge = badge;
            }

            return new Response(JSON.stringify(responseData), { headers: baseHeaders });
        } // End /api/submit

        // --- Fallback for unmatched API routes ---
        console.warn(`API route not found: /api/${apiPath}`);
        return new Response(JSON.stringify({ error: `API 路由 /api/${apiPath} 未找到` }), { status: 404, headers: baseHeaders });

    } catch (error: any) {
        console.error(`Unhandled error processing /api/${apiPath}:`, error);
        // ... (Error handling remains the same)
        const status = (typeof error.status === 'number' && error.status >= 400 && error.status < 600) ? error.status : 500;
        const specificError = error.message || '伺服器內部發生未知錯誤';
        const errorMessage = (status < 500) ? `請求處理錯誤: ${specificError}` : `伺服器內部錯誤 (${status})，請稍後再試或聯繫管理員。`;
        if (status >= 500) console.error(`Responding with Internal Server Error (${status}). Error: ${specificError}. Stack:`, error.stack);
        else console.warn(`Responding with Client Error (${status}). Error: ${specificError}`);
        return new Response(JSON.stringify({ error: errorMessage }), { status: status, headers: baseHeaders });
    }
}; // End onRequest Handler


// --- Helper function to convert rank to Chinese numerals (Unchanged) ---
function convertToChineseRank(rank: number): string {
    // ... (Implementation unchanged)
    if (rank <= 0) return "零";
    const chineseNumbers = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十"];
    if (rank <= 10) return chineseNumbers[rank];
    else if (rank < 20) return "十" + chineseNumbers[rank - 10];
    else if (rank % 10 === 0 && rank < 100) return chineseNumbers[Math.floor(rank / 10)] + "十";
    else if (rank < 100) return chineseNumbers[Math.floor(rank / 10)] + "十" + chineseNumbers[rank % 10];
    else return `${rank}`;
}