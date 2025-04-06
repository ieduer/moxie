import type { PagesFunction, R2Bucket, KVNamespace } from '@cloudflare/workers-types';

// --- NEW DATA LOADING --- (Requirement 1 & 2)
// Load pre-defined questions from moni.json
import moniDataRaw from '../../data/moni.json';

// --- Constants ---
const KV_EXPIRATION_TTL_SECONDS = 3600; // 1 hour
// ** 使用用戶指定的模型名稱 **
// 注意：如果 Vision 模型不支持長上下文或 JSON 模式，OCR 或 反饋可能出錯
const GEMINI_VISION_MODEL = "gemini-2.0-flash-thinking-exp-01-21"; // Changed to a potentially more stable/available model
const GEMINI_TEXT_MODEL = "gemini-2.0-flash-thinking-exp-01-21";    // Changed to a potentially more stable/available model
const GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
const MAX_QUESTIONS_PER_SET = 4; // Target number of questions from moni.json
const MAX_FEEDBACK_TOKENS = 3000; // Increased token limit for feedback (was 600)
const API_RETRY_COUNT = 2; // Number of retries for API calls
const API_RETRY_DELAY_MS = 1500; // Initial delay for retries

// --- Type Definitions ---

// (Requirement 1 & 2) Define type for items in moni.json
interface MoniQuestion {
    type: string;
    question: string;
    reference_answer: string; // Key name matches the JSON file
}

// For a single generated question object (Adapted for moni.json)
interface QuestionInfo {
    id: string; // Unique ID for this specific question instance in the set
    question: string; // The question text from moni.json
    answer: string; // The reference_answer from moni.json
    source?: string; // Added to match generatedQuestions mapping
    topic?: string; // Added potential field
}


// For the entire set stored in KV
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
// Updated GeminiApiResponse to better reflect potential error structures
interface GeminiErrorDetail { code: number; message: string; status: string; }
interface GeminiApiResponse {
    candidates?: GeminiCandidate[];
    promptFeedback?: any;
    // Google API errors might be nested under 'error'
    error?: GeminiErrorDetail;
}


// For scoring results returned to frontend (Keep as they are)
interface SubmissionResult {
    questionIndex: number;
    questionId: string; // Link back to the QuestionInfo id
    success: boolean; // Indicates if OCR and processing for this item was okay
    recognizedText: string;
    correctAnswer: string;
    isCorrect: boolean;
    score: number;
    error?: string; // Specific error message for this item, if any
}

// Environment Bindings Interface provided by Cloudflare (Keep as they are)
interface Env {
    IMAGES_BUCKET: R2Bucket;
    GEMINI_API_KEY: string;
    SESSION_KV: KVNamespace;
}

// --- Utility Functions --- (Keep getRandomItems, arrayBufferToBase64, generateUniqueKey)

function getRandomItems<T>(arr: T[], num: number): T[] {
    if (!Array.isArray(arr)) {
        console.error("getRandomItems: input is not an array", arr);
        return [];
    }
    if (num >= arr.length) {
        // If requesting more or equal items than available, return a shuffled copy of the whole array
        const shuffledAll = [...arr];
        for (let i = shuffledAll.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [shuffledAll[i], shuffledAll[j]] = [shuffledAll[j], shuffledAll[i]];
        }
        return shuffledAll;
    }
    // Fisher-Yates (Knuth) Shuffle for better randomness when selecting a subset
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
     // Use crypto.randomUUID for better uniqueness than Math.random
     return `${prefix}-${Date.now()}-${crypto.randomUUID()}${extension}`;
}

// Helper function for async delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// --- Gemini API Call Function (Corrected Retry Logic & clonedResponse Scope) ---
async function callGeminiAPI(apiKey: string, model: string, contents: GeminiContent[], generationConfig?: { maxOutputTokens?: number; temperature?: number; }): Promise<GeminiApiResponse> {
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
                    // Consider if this needs specific handling or if returning is sufficient
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

// --- **TYPE GUARD for moni.json data** --- (Remains the same)
function isValidMoniData(data: any): data is MoniQuestion[] {
    // ... (implementation is unchanged)
    console.log("isValidMoniData: Function called");

    if (!Array.isArray(data)) {
        const errorMessage = "Data is not an array."; // More specific error
        console.error("isValidMoniData: Input is not an array.", data);
        // Include detail in the returned error message
        throw new Error(errorMessage);
    }
    if (data.length === 0) {
        const errorMessage = "Data array is empty."; // More specific error
        console.error("isValidMoniData: Array is empty.");
        // Include detail in the returned error message
        throw new Error(errorMessage);
    }
    const sample = data[0];
    const isValid = typeof sample?.type === 'string' &&
                    typeof sample?.question === 'string' &&
                    typeof sample?.reference_answer === 'string';
    if (!isValid) {
        const errorMessage = "First item in data array is invalid. Structure is incorrect."; // More specific
        console.error("isValidMoniData: First item structure is invalid.", sample);
        console.error("Types:", {
            type: typeof sample?.type,
            question: typeof sample?.question,
            reference_answer: typeof sample?.reference_answer
        });
        // Include detail in the returned error message
        throw new Error(errorMessage);
    }
    return isValid;
}


// --- Main Request Handler ---
export const onRequest: PagesFunction<Env> = async (context) => {
    // ... (Initial setup, headers, OPTIONS handling, env check remain the same)
    console.log("onRequest: API request received");
    const { request, env, params } = context;
    const url = new URL(request.url);
    const apiPath = (params.path as string[] || []).join('/');

    const baseHeaders = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    };

    if (request.method === 'OPTIONS') {
        return new Response(null, { headers: baseHeaders });
    }

    console.log(`[${new Date().toISOString()}] Request: ${request.method} /api/${apiPath}`);

    if (!env.IMAGES_BUCKET || !env.GEMINI_API_KEY || !env.SESSION_KV) {
         console.error("FATAL: Server configuration error: Missing required Cloudflare bindings (R2, KV) or secrets (GEMINI_API_KEY).");
         return new Response(JSON.stringify({ error: "Server configuration error. Please contact administrator." }), { status: 500, headers: baseHeaders });
    }

    // --- **DATA VALIDATION** --- (Remains the same)
    let moniQuestions: MoniQuestion[];
    if (isValidMoniData(moniDataRaw)) {
        moniQuestions = moniDataRaw;
        console.log(`Successfully validated moni.json data. Found ${moniQuestions.length} questions.`);
    } else {
        console.error("FATAL: moni.json data failed validation. Check the file structure and content.");
        // Return error immediately if essential data is invalid
        return new Response(JSON.stringify({ error: "內部題庫數據格式錯誤，無法提供服務。" }), { status: 500, headers: baseHeaders });
    }

    try {
        // --- API Routing ---

        // ... ('/hello' route remains the same)
        if (apiPath === 'hello' && request.method === 'GET') {
            const dataInfo = {
                message: "Backend is running.",
                status: "OK",
                timestamp: new Date().toISOString(),
                moniQuestionsLoaded: moniQuestions.length, // Report count from validated moni data
                modelsUsed: { vision: GEMINI_VISION_MODEL, text: GEMINI_TEXT_MODEL },
                maxFeedbackTokens: MAX_FEEDBACK_TOKENS
            };
            return new Response(JSON.stringify(dataInfo), { headers: baseHeaders });
        }

        // ... ('/start_set' route remains the same)
        if (apiPath === 'start_set' && request.method === 'GET') {
            console.log("Processing /api/start_set request using pre-defined moni.json");

            // Check if we have enough questions in the validated data
            if (moniQuestions.length < MAX_QUESTIONS_PER_SET) {
                throw new Error(`題庫中的題目數量 (${moniQuestions.length}) 不足 ${MAX_QUESTIONS_PER_SET} 道，無法出題。`);
            }

            const setId = crypto.randomUUID();

            // Select 4 unique random questions from moniQuestions
            const selectedMoniQuestions = getRandomItems(moniQuestions, MAX_QUESTIONS_PER_SET);

            // Map the selected questions to the QuestionInfo format
            const generatedQuestions: QuestionInfo[] = selectedMoniQuestions.map((moniQ, index) => {
                return {
                    id: crypto.randomUUID(), // Generate unique ID for this instance
                    question: moniQ.question,
                    answer: moniQ.reference_answer,
                    // Keep track of the original type, maybe useful for frontend display or filtering later
                    source: moniQ.type // Use the type field as the source category
                    // topic field could be added here if available in moni.json or derived
                };
            });

            console.log(`Selected ${generatedQuestions.length} questions from moni.json for setId: ${setId}`);

             // Store the generated set in KV
             const newSet: QuestionSet = { setId, questions: generatedQuestions, createdAt: Date.now() };
             try {
                 await env.SESSION_KV.put(setId, JSON.stringify(newSet), { expirationTtl: KV_EXPIRATION_TTL_SECONDS });
                 console.log(`Stored new question set in KV with setId: ${setId} (${generatedQuestions.length} questions)`);
             } catch (kvError: any) {
                 console.error(`KV put error for setId ${setId}:`, kvError);
                 throw new Error(`無法保存生成的題組信息: ${kvError.message}`);
             }

             // Prepare response for the frontend (without answers)
             const questionsForFrontend = newSet.questions.map(({ answer, ...rest }: QuestionInfo) => rest);
             return new Response(JSON.stringify({ setId: newSet.setId, questions: questionsForFrontend }), { headers: baseHeaders });

        } // End /api/start_set (moni.json version)


        // --- '/submit' route ---
        if (apiPath === 'submit' && request.method === 'POST') {
            // ... (Request parsing, validation, KV retrieval, R2 storage remain the same)
            console.log("Processing /api/submit request");

            // --- Request Parsing and Validation ---
            const formData = await request.formData();
            const setIdValue = formData.get('setId');
            const imageValue = formData.get('handwritingImage');
            let imageFile: File;

            // Validate setId
            if (typeof setIdValue !== 'string' || !setIdValue) {
                 console.error("Invalid submit request: Missing or invalid setId.", { setIdValue });
                 return new Response(JSON.stringify({ error: '請求無效：缺少有效的題組 ID。' }), { status: 400, headers: baseHeaders });
            }
            const setId: string = setIdValue;

            // Validate imageValue
            if (!imageValue) {
                console.error("Invalid submit request: Missing imageValue.", { setId });
                return new Response(JSON.stringify({ error: '請求無效：未上傳圖片。' }), { status: 400, headers: baseHeaders });
            }
            if (typeof imageValue === 'string') {
                console.error("Invalid submit request: Uploaded value is a string, expected File.", { setId });
                return new Response(JSON.stringify({ error: '請求錯誤：上傳的數據格式不正確（應為文件）。' }), { status: 400, headers: baseHeaders });
            }
            const tempImageFile = imageValue as File;
            if (tempImageFile.size === 0) {
                 console.error("Invalid submit request: Uploaded File is empty.", { setId, fileName: tempImageFile.name });
                 return new Response(JSON.stringify({ error: '請求無效：上傳的圖片文件大小為 0。' }), { status: 400, headers: baseHeaders });
            }
            imageFile = tempImageFile;
            console.log(`Validation passed for setId: ${setId}. Image: ${imageFile.name}, Size: ${imageFile.size}, Type: ${imageFile.type}`);

            // --- Retrieve Question Set from KV ---
            let questionSet: QuestionSet | null = null;
            try {
                questionSet = await env.SESSION_KV.get<QuestionSet>(setId, 'json');
            } catch (kvError: any) {
                console.error(`KV get error for setId ${setId}:`, kvError);
                return new Response(JSON.stringify({ error: "無法獲取題組信息，會話可能已過期或ID無效，請重新開始挑戰。" }), { status: 404, headers: baseHeaders });
            }
            if (!questionSet || !questionSet.questions || questionSet.questions.length === 0 ) {
                console.error(`Invalid or missing/empty question set data in KV for setId ${setId}`, questionSet);
                return new Response(JSON.stringify({ error: "無效的題組信息，請重新開始挑戰。" }), { status: 400, headers: baseHeaders });
            }
            const expectedQuestionCount = questionSet.questions.length; // Should be 4
            console.log(`Found ${expectedQuestionCount} questions for setId ${setId}.`);

            const correctAnswers = questionSet.questions.map((q: QuestionInfo) => q.answer);
            const questionIds = questionSet.questions.map((q: QuestionInfo) => q.id);
            console.log(`Retrieved correct answers for setId ${setId}`);

            // --- Store Image to R2 ---
            const imageBuffer = await imageFile.arrayBuffer();
            const r2Key = generateUniqueKey(`set-${setId}-answer`, `.${imageFile.type.split('/')[1] || 'png'}`);
            try {
                 await env.IMAGES_BUCKET.put(r2Key, imageBuffer, { httpMetadata: { contentType: imageFile.type }});
                 console.log(`Stored image in R2 with key: ${r2Key} for setId: ${setId}`);
            } catch (r2Error: any) {
                 console.error(`R2 put error for key ${r2Key}:`, r2Error);
                 // Return error response if R2 fails
                  return new Response(JSON.stringify({ error: `圖片存儲失敗: ${r2Error.message || 'Unknown R2 error'}` }), { status: 500, headers: baseHeaders });
            }


            // --- Call Gemini Vision for OCR ---
            const base64ImageData = arrayBufferToBase64(imageBuffer);
            const ocrStartTime = Date.now();
            let recognizedTextCombined = '';
            let ocrError: string | null = null;
            let splitAnswers: string[] = [];
            const ocrContents: GeminiContent[] = [{
                parts: [
                    // Updated prompt to handle exactly 4 answers
                    { "text": `这是一张包含${expectedQuestionCount}个手写简体中文答案的图片，按从上到下的顺序排列。请准确识别每个答案，并只用换行符（\\n）分隔返回${expectedQuestionCount}个结果。不要添加任何其他文字、解释、编号或格式。如果某个答案无法识别，请在那一行输出 "[無法識別]"。` },
                    { "inline_data": { "mime_type": imageFile.type || "image/png", "data": base64ImageData } }
                ]
            }];

            try {
                // Use the updated callGeminiAPI with retry logic
                const geminiResult = await callGeminiAPI(env.GEMINI_API_KEY, GEMINI_VISION_MODEL, ocrContents, { maxOutputTokens: 800, temperature: 0.1 });
                const ocrDuration = Date.now() - ocrStartTime;
                console.log(`Gemini OCR completed for setId ${setId} in ${ocrDuration}ms.`);

                const candidate = geminiResult.candidates?.[0];
                const part = candidate?.content?.parts?.[0];

                 // Check finish reason for OCR as well
                 const ocrFinishReason = candidate?.finishReason;
                 console.log(`Gemini OCR candidate finish reason: ${ocrFinishReason} for setId: ${setId}`);
                 if (ocrFinishReason && ocrFinishReason !== "STOP") {
                     console.warn(`OCR process potentially incomplete. Finish Reason: ${ocrFinishReason}`);
                     // Optionally add this to ocrError
                     ocrError = ocrError ? `${ocrError}. ` : ''; // Append if error exists
                     ocrError += `AI處理可能未完成 (${ocrFinishReason})。`;
                 }


                if (part && 'text' in part) {
                    recognizedTextCombined = part.text.trim();
                } else if (geminiResult.error) { // Check explicit error structure first
                     ocrError = `AI OCR 服務錯誤: ${geminiResult.error.message}`;
                     console.error(`OCR API Error from structure for setId ${setId}:`, geminiResult.error);
                } else {
                    ocrError = "AI OCR 返回了非預期的響應格式 (無文本部分)。";
                    console.warn(`OCR Result format issue for setId ${setId}. Full Response:`, JSON.stringify(geminiResult));
                }

                if (!ocrError && !recognizedTextCombined) {
                     // If no text and no specific error, check finish reason again
                     if (ocrFinishReason === "SAFETY") {
                         ocrError = "AI OCR 因安全設置拒絕處理圖片內容。";
                     } else if (ocrFinishReason === "RECITATION") {
                         ocrError = "AI OCR 因檢測到引用內容而停止。";
                     } else if (ocrFinishReason === "MAX_TOKENS") {
                         ocrError = "AI OCR 處理超時或輸出長度受限。";
                     } else {
                         ocrError = "AI OCR 未能識別出任何文本內容。";
                     }
                    console.warn(`OCR Result empty for setId ${setId}. Finish Reason: ${ocrFinishReason}`);
                } else if (!ocrError) {
                    console.log(`Raw OCR result for setId ${setId}: "${recognizedTextCombined.replace(/\n/g, '\\n')}"`);
                    // Ensure splitting handles potential empty lines robustly
                    splitAnswers = recognizedTextCombined.split('\n').map(s => s.trim()); //.filter(s => s); <- Keep empty strings initially for count

                    if (splitAnswers.length !== expectedQuestionCount) {
                        console.warn(`OCR split count mismatch for setId ${setId}: expected ${expectedQuestionCount}, got ${splitAnswers.length}. Raw: "${recognizedTextCombined}"`);
                        // Try to be smarter about padding/truncating based on raw text if possible
                        ocrError = `AI OCR 未能準確分割出 ${expectedQuestionCount} 個答案 (找到了 ${splitAnswers.length} 個)。答案可能擠在一起或部分無法識別。`;
                        // Pad or truncate to match the expected number, using a clearer placeholder
                        while (splitAnswers.length < expectedQuestionCount) splitAnswers.push("[答案缺失]");
                        if (splitAnswers.length > expectedQuestionCount) splitAnswers = splitAnswers.slice(0, expectedQuestionCount);
                    } else {
                        console.log(`Successfully split OCR into ${splitAnswers.length} answers for setId ${setId}.`);
                    }
                }
            } catch (err: any) {
                 console.error(`Gemini OCR API call failed for setId ${setId}:`, err);
                 ocrError = `AI OCR 識別服務調用失敗: ${err.message}`;
                 // Populate splitAnswers with failure message for all questions
                 splitAnswers = Array(expectedQuestionCount).fill(`[OCR調用失敗]`); // Use a distinct message
            }

            // --- Scoring --- (Remains largely the same logic)
            const results: SubmissionResult[] = [];
            let totalScore = 0;
            const pointsPerQuestion = expectedQuestionCount > 0 ? (8 / expectedQuestionCount) : 0;

            function removePunctuation(text: string): string {
                if (typeof text !== 'string') return text;
                return text.replace(/[\p{P}\p{S}\p{Z}]+/gu, '');
            }

            for (let i = 0; i < expectedQuestionCount; i++) {
                const recognized = splitAnswers[i] !== undefined ? splitAnswers[i] : "[答案缺失]"; // Handle potential undefined
                const correct = correctAnswers[i];
                const questionId = questionIds[i];
                let isCorrect = false;
                let score = 0;
                // Assume success unless explicit failure placeholder from OCR step
                let success = !recognized.startsWith("[") || recognized === "[無法識別]";
                let itemError: string | undefined = undefined;

                // Map OCR failure placeholders to errors and non-success
                if (recognized === "[OCR調用失敗]" || recognized === "[答案提取失敗]" || recognized === "[答案缺失]") {
                    itemError = recognized.substring(1, recognized.length - 1);
                    success = false;
                } else if (recognized === "[無法識別]") {
                     itemError = "AI 無法識別此答案";
                     success = false; // Mark as unsuccessful if unrecognizable
                 }


                // Perform comparison only if OCR was successful for this item
                if (success && correct !== undefined) {
                    const cleanedRecognized = removePunctuation(recognized);
                    const cleanedCorrect = removePunctuation(correct);
                    // Check for non-empty comparison
                    isCorrect = cleanedRecognized === cleanedCorrect && cleanedRecognized !== "";

                     if (!isCorrect && cleanedRecognized === "" && recognized !== "") {
                         // Recognized contained only punctuation/whitespace
                         itemError = "識別結果僅包含標點或空格";
                     } else if (recognized.trim() === "" && !itemError) {
                         // Truly empty answer might indicate not attempted
                         itemError = "未作答或未識別到內容";
                         // Consider if this should be success=false? Depends on definition.
                         // For now, keeping success=true if OCR didn't explicitly fail.
                     }

                    score = isCorrect ? pointsPerQuestion : 0;
                } else {
                    // If OCR failed (success is false) or correct answer is missing
                    isCorrect = false;
                    score = 0;
                    if (correct === undefined) {
                         itemError = itemError ? `${itemError}; 標準答案缺失` : "標準答案缺失";
                         success = false; // Definitely not successful if standard answer missing
                    }
                 }

                results.push({
                     questionIndex: i,
                     questionId: questionId,
                     success: success,
                     recognizedText: recognized, // Keep original for display
                     correctAnswer: correct || "[標準答案缺失]",
                     isCorrect: isCorrect,
                     score: score,
                     error: itemError
                });
                totalScore += score;
            }
            totalScore = Math.round(totalScore * 10) / 10; // Round score
            console.log(`Scoring complete for setId ${setId}. Total score: ${totalScore} / 8`);

            // --- Generate Feedback using AI ---
            let feedback = "";
            const feedbackStartTime = Date.now();
            let feedbackErrorMsg: string | null = null;
            let feedbackFinishReason: string | null = null; // Track feedback finish reason

            // ... (Rank/Badge logic remains the same)
            let currentRank = 0; // 默認階級
            let badge = "";
            const rankKey = `user-rank-${setId}`; // 使用 setId 作為 session 標識

            // --- (Requirement 6) Get Current Rank from KV ---
            try {
                const storedRank = await env.SESSION_KV.get(rankKey, 'text');
                if (storedRank) {
                    currentRank = parseInt(storedRank, 10) || 0;
                }
            } catch (kvRankError) {
                console.error("Failed to get rank from KV:", kvRankError);
                // 忽略錯誤，使用默認階級 0
            }


            if (totalScore === 8) {
                 feedback = `非常好！滿分 8 分！簡直是MXDS（默寫的神）！繼續保持！`;
                 feedbackErrorMsg = null;
                 // --- Rank Increase and Badge ---
                 currentRank++;
                 badge = `${convertToChineseRank(currentRank)}階`;
                 if (currentRank === 1) badge = `初窺門徑`; // Special first rank
                 if (currentRank >= 7) badge = `巔峰七階`; // Cap badge display?


            } else {
                // Prepare details for the gentle prompt
                const incorrectResults = results.filter(r => !r.isCorrect);
                const errorDetails = incorrectResults
                    .map((r: SubmissionResult) => {
                        let reason = r.error ? `(原因: ${r.error})` : '(內容錯誤)';
                        // Make reasons clearer
                        if (r.recognizedText === '[無法識別]') reason = '(字跡無法識別)';
                        else if (r.recognizedText === '[答案缺失]') reason = '(未找到對應答案)';
                        else if (r.recognizedText === '[答案提取失敗]') reason = '(答案提取過程失敗)';
                        else if (r.recognizedText === '[OCR調用失敗]') reason = '(圖片識別過程失敗)';
                        else if (removePunctuation(r.recognizedText) === '') reason = '(未作答或僅有標點)';

                        return `第 ${r.questionIndex + 1} 題 ${reason}:\n  你的答案: "${r.recognizedText}"\n  正確答案: "${r.correctAnswer}"`;
                    })
                    .join('\n\n');

                const feedbackPrompt = `你扮演一位非常溫和、有耐心的高考語文老師，你的目標是幫助學生從錯誤中學習，建立信心。學生這次默寫考試（滿分8分）沒有拿到滿分，得分 ${totalScore.toFixed(1)}，失分 ${(8-totalScore).toFixed(1)}。你需要用充滿鼓勵和關懷的語氣來進行點評。

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


                try {
                    console.log(`Generating Gentle AI feedback for setId ${setId}...`);
                    const feedbackContents: GeminiContent[] = [{ parts: [{ text: feedbackPrompt }] }];
                    // Use updated callGeminiAPI with retry logic and increased token limit
                    const feedbackResult = await callGeminiAPI(
                        env.GEMINI_API_KEY,
                        GEMINI_TEXT_MODEL,
                        feedbackContents,
                        { maxOutputTokens: MAX_FEEDBACK_TOKENS, temperature: 0.8 }
                    );

                    // --- **ROBUST RESPONSE PROCESSING** ---
                    let generatedText: string | null = null;
                    let extractionFailureReason = "Unknown structure issue";

                    const candidate = feedbackResult.candidates?.[0];
                    feedbackFinishReason = candidate?.finishReason ?? null; // Store finish reason
                    console.log(`Gemini feedback generation candidate finish reason: ${feedbackFinishReason} for setId: ${setId}`);


                    if (candidate?.content?.parts?.[0] && 'text' in candidate.content.parts[0]) {
                        const trimmedText = candidate.content.parts[0].text.trim();
                        if (trimmedText.length > 0) {
                            generatedText = trimmedText;
                            extractionFailureReason = "";
                        } else {
                            extractionFailureReason = "Extracted text is empty after trimming.";
                            console.warn(`AI feedback generation returned an empty text string for setId ${setId}. Finish Reason: ${feedbackFinishReason}`);
                        }
                    } else if (feedbackResult.error) { // Check explicit error structure
                         extractionFailureReason = `API Error in response: ${feedbackResult.error.message}`;
                         console.error(`AI feedback generation encountered an API error in the response structure for setId ${setId}:`, feedbackResult.error);
                    }
                    else {
                         // Different potential failures
                         if (!feedbackResult.candidates || feedbackResult.candidates.length === 0) {
                             extractionFailureReason = "Response contains no candidates.";
                         } else if (!candidate?.content?.parts || candidate.content.parts.length === 0) {
                             extractionFailureReason = "Candidate content or parts array is missing or empty.";
                         } else {
                             extractionFailureReason = "First part exists but is not a text part.";
                         }
                         console.warn(`AI feedback generation: ${extractionFailureReason} for setId ${setId}. Full Response:`, JSON.stringify(feedbackResult));
                    }

                    // Handle finish reason issues
                    if (feedbackFinishReason && feedbackFinishReason !== "STOP" && generatedText !== null) {
                        // If text was generated but finish reason is not STOP, append a warning
                        let reasonWarning = "";
                        if (feedbackFinishReason === "MAX_TOKENS") reasonWarning = "回覆可能因長度限制被截斷。";
                        else if (feedbackFinishReason === "SAFETY") reasonWarning = "回覆可能因安全設置被部分過濾。";
                        else if (feedbackFinishReason === "RECITATION") reasonWarning = "回覆可能因檢測到引用內容而提前終止。";
                        else reasonWarning = `回覆處理因 (${feedbackFinishReason}) 而結束。`;
                        feedback = `${generatedText}\n\n[系統提示: ${reasonWarning}]`;
                        feedbackErrorMsg = reasonWarning; // Also report as an issue
                    } else if (generatedText !== null) {
                         // Success
                         feedback = generatedText;
                         feedbackErrorMsg = null;
                         console.log(`AI feedback (gentle) generated and extracted successfully for setId ${setId}.`);
                     }
                     else {
                        // Failed to extract text
                        console.error(`Failed to extract valid AI feedback text for setId ${setId}. Reason: ${extractionFailureReason}. Finish Reason: ${feedbackFinishReason}. Using fallback.`);
                        console.error("Full Gemini Response causing fallback:", JSON.stringify(feedbackResult, null, 2));

                        let fallbackReason = extractionFailureReason;
                         if (!fallbackReason.toLowerCase().includes("api error")) {
                             // Add finish reason info if it's not STOP and not already part of the error
                             if (feedbackFinishReason && feedbackFinishReason !== "STOP") {
                                fallbackReason += ` (處理結束原因: ${feedbackFinishReason})`;
                             }
                         }

                        feedbackErrorMsg = `AI 反饋生成成功，但內容提取失敗 (${fallbackReason})。`;
                        feedback = `得分 ${totalScore.toFixed(1)}，失分 ${(8 - totalScore).toFixed(1)}。這次表現還有進步空間哦。看看下面的錯誤細節，下次加油！\n錯誤詳情:\n${errorDetails || "（未能生成詳細的錯誤分析）"}`;
                    }

                } catch (feedbackError: any) {
                     // API call itself failed (after retries)
                     console.error(`Gemini feedback generation failed for setId ${setId}:`, feedbackError);
                     feedbackErrorMsg = `AI 反饋生成服務調用失敗: ${feedbackError.message}`;
                     feedback = `得分 ${totalScore.toFixed(1)}，失分 ${(8 - totalScore).toFixed(1)}。這次表現還有進步空間哦。看看下面的錯誤細節，下次加油！\n錯誤詳情:\n${errorDetails || "（未能生成詳細的錯誤分析）"}`;
                }

                 // --- Rank Decrease ---
                 if (currentRank > 0) {
                     currentRank--;
                 }
                 badge = currentRank > 0 ? `${convertToChineseRank(currentRank)}階` : "初窺門徑";
                  if (currentRank >= 7) badge = `巔峰七階`; // Cap badge display?
            }

            // --- Store Updated Rank ---
            try {
                await env.SESSION_KV.put(rankKey, String(currentRank), { expirationTtl: KV_EXPIRATION_TTL_SECONDS });
                console.log(`Rank updated to ${currentRank} for setId ${setId}`);
            } catch (kvPutRankError) {
                console.error("Failed to put rank to KV:", kvPutRankError);
            }

            // --- Prepare Final Response ---
            let finalMessage = "評分完成。";
            if (ocrError && feedbackErrorMsg) {
                finalMessage = "評分完成，但圖片識別和 AI 反饋生成均遇到問題。";
            } else if (ocrError) {
                finalMessage = "評分完成，但圖片識別過程遇到問題。";
            } else if (feedbackErrorMsg) {
                 // Include finish reason details in the message if relevant
                 let feedbackIssueDetail = feedbackErrorMsg;
                 if (feedbackFinishReason && feedbackFinishReason !== "STOP" && !feedbackErrorMsg.includes(feedbackFinishReason)) {
                     feedbackIssueDetail += ` (原因: ${feedbackFinishReason})`;
                 }
                 finalMessage = `評分完成，但 AI 反饋生成過程遇到問題: ${feedbackIssueDetail}`;
            }


            const responseData = {
                message: finalMessage,
                totalScore: totalScore,
                results: results,
                feedback: feedback, // Contains combined feedback + system warnings if needed
                r2Key: r2Key,
                ocrIssue: ocrError, // Contains specific OCR error
                feedbackIssue: feedbackErrorMsg, // Contains specific feedback error/warning
                rank: currentRank,
                badge: badge
            };
            return new Response(JSON.stringify(responseData), { headers: baseHeaders });
        } // End /api/submit

        // --- Fallback for unmatched API routes ---
        console.warn(`API route not found: /api/${apiPath}`);
        return new Response(JSON.stringify({ error: `API 路由 /api/${apiPath} 未找到` }), { status: 404, headers: baseHeaders });

    } catch (error: any) {
        console.error(`Unhandled error processing /api/${apiPath}:`, error);
        const status = (typeof error.status === 'number' && error.status >= 400 && error.status < 600) ? error.status : 500;
        const specificError = error.message || '伺服器內部發生未知錯誤';
        const errorMessage = (status < 500) ? `請求處理錯誤: ${specificError}` : `伺服器內部錯誤 (${status})，請稍後再試或聯繫管理員。`;

        if (status >= 500) {
            console.error(`Responding with Internal Server Error (${status}). Error: ${specificError}. Stack:`, error.stack);
        } else {
            console.warn(`Responding with Client Error (${status}). Error: ${specificError}`);
        }
        return new Response(JSON.stringify({ error: errorMessage }), { status: status, headers: baseHeaders });
    }
}; // End onRequest Handler


// --- Helper function to convert rank to Chinese numerals --- (Remains the same)
function convertToChineseRank(rank: number): string {
    if (rank <= 0) return "零";

    const chineseNumbers = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十"];
    if (rank <= 10) {
        return chineseNumbers[rank];
    } else if (rank < 20) {
        return "十" + chineseNumbers[rank - 10];
    } else if (rank % 10 === 0 && rank < 100) {
        return chineseNumbers[Math.floor(rank / 10)] + "十";
    } else if (rank < 100) {
        return chineseNumbers[Math.floor(rank / 10)] + "十" + chineseNumbers[rank % 10];
    } else {
        return `${rank}`; // Default for higher numbers
    }
}