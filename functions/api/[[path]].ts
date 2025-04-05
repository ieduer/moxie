import type { PagesFunction, R2Bucket, KVNamespace } from '@cloudflare/workers-types';

// --- Data Loading ---
// Ensure these paths are correct relative to the functions directory root
import zhentiData from '../../data/zhenti.json'; // Keep for potential future reference
// Assuming kaoshifanwei.json has the structure { "文言文": [...], "诗词曲": [...] }
import kaoshifanweiRawData from '../../data/kaoshifanwei.json';

// --- Constants ---
const KV_EXPIRATION_TTL_SECONDS = 3600; // 1 hour
const GEMINI_VISION_MODEL = "gemini-1.5-pro-latest"; // Or "gemini-pro-vision"
const GEMINI_TEXT_MODEL = "gemini-pro"; // Standard text model for feedback
const GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
const MAX_QUESTIONS_PER_SET = 4; // Default target number of questions
const MIN_LINES_FOR_QUESTION = 2; // Minimum consecutive lines needed after splitting content

// --- Type Definitions ---

// For items within the raw JSON structure (e.g., inside "文言文" array)
interface KaoshiFanweiSourceItem {
    id?: number | string; // Allow string IDs if they exist
    title: string;
    author?: string; // Optional author
    dynasty?: string; // Optional dynasty
    content: string; // Expecting a single string with newlines based on previous errors
}

// For the overall structure of the imported kaoshifanweiRawData
interface KaoshiFanweiRawStructure {
    文言文?: KaoshiFanweiSourceItem[]; // Use optional keys if categories might vary
    诗词曲?: KaoshiFanweiSourceItem[];
    // Add other categories if present, e.g., '其他'?: KaoshiFanweiSourceItem[];
}

// Target flat structure used by the generator function
interface KaoshiFanweiItem {
    id: string; // Unique ID generated during flattening
    title: string;
    author?: string;
    dynasty?: string;
    content: string[]; // Array of lines after splitting
    category: string; // Store the original category
}

// For a single generated question object
interface QuestionInfo {
    id: string; // Unique ID for this specific question instance
    question: string; // The generated question text (e.g., with blanks)
    answer: string; // The correct answer derived from the source text
    source: string; // Info about the original text (e.g., title, author)
    topic?: string; // Default topic description
}

// For the entire set stored in KV
interface QuestionSet {
    setId: string; // Unique ID for the entire set
    questions: QuestionInfo[]; // Array of generated questions
    createdAt: number; // Timestamp
}

// Gemini API related types
interface GeminiTextPart { text: string; }
interface GeminiImageDataPart { inline_data: { mime_type: string; data: string; }; }
interface GeminiContent { parts: (GeminiTextPart | GeminiImageDataPart)[]; role?: string; }
interface GeminiCandidate { content: GeminiContent; finishReason?: string; index?: number; safetyRatings?: any[]; }
interface GeminiApiResponse { candidates?: GeminiCandidate[]; promptFeedback?: any; error?: { code: number; message: string; status: string }; }

// For scoring results returned to frontend
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

// Environment Bindings Interface provided by Cloudflare
interface Env {
    IMAGES_BUCKET: R2Bucket;       // R2 Binding
    GEMINI_API_KEY: string;        // Secret Variable
    SESSION_KV: KVNamespace;       // KV Binding
}

// --- Utility Functions ---

function getRandomItems<T>(arr: T[], num: number): T[] {
    if (!Array.isArray(arr)) {
        console.error("getRandomItems: input is not an array", arr);
        return [];
    }
    // Fisher-Yates (Knuth) Shuffle for better randomness
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

// --- Data Flattening Function ---

function flattenKaoshiFanweiData(rawData: KaoshiFanweiRawStructure): KaoshiFanweiItem[] {
    const flatArray: KaoshiFanweiItem[] = [];
    let counter = 0; // Simple counter for fallback unique ID generation

    // Iterate over the known categories (or all keys if structure is less defined)
    for (const category in rawData) {
        // Ensure the key is own property and its value is an array
        if (Object.prototype.hasOwnProperty.call(rawData, category) && Array.isArray(rawData[category as keyof KaoshiFanweiRawStructure])) {
            // Cast the array items to the expected source item type
            const items = rawData[category as keyof KaoshiFanweiRawStructure] as KaoshiFanweiSourceItem[] | undefined;
             if (items) {
                 items.forEach(item => {
                    // Split the content string into lines, trim, and filter empty ones
                    const lines = typeof item.content === 'string'
                                  ? item.content.split('\n').map(line => line.trim()).filter(line => line.length > 0)
                                  : []; // Default to empty array if content is not a string

                    // Only include items that have enough lines *after splitting*
                     if (lines.length >= MIN_LINES_FOR_QUESTION) {
                         flatArray.push({
                            // Generate a more robust unique ID using category and original ID/counter
                            id: `${category}-${item.id ?? counter++}`,
                            title: item.title,
                            author: item.author,
                            dynasty: item.dynasty,
                            content: lines, // Assign the processed array of lines
                            category: category // Store the original category
                        });
                     } else {
                          // Log items skipped due to insufficient lines *after splitting*
                          if (typeof item.content === 'string') {
                               console.warn(`Skipping item: ${item.title} (ID: ${item.id ?? 'N/A'}) - Insufficient lines (${lines.length}) after splitting content.`);
                          } else {
                               console.warn(`Skipping item: ${item.title} (ID: ${item.id ?? 'N/A'}) - Invalid or missing content property.`);
                          }
                     }
                 });
             }
        }
    }
    console.log(`Flattened kaoshifanwei data: ${flatArray.length} valid items with >= ${MIN_LINES_FOR_QUESTION} lines.`);
    return flatArray;
}


// --- Question Generation Logic ---

// Defines the signature for a function that creates a question from lines
type QuestionPattern = (lines: string[], sourceInfo: string) => QuestionInfo | null;

// Pattern: Fill in the second part of a sentence (e.g., "ABC，____。")
const patternFillNext: QuestionPattern = (lines, sourceInfo) => {
    if (lines.length < 1) return null;
    const line = lines[0];
    // Simple regex split attempt for common cases
    const parts = line.match(/^(.*?[，、；])(.*?[。？！])$/);
    if (parts && parts.length === 3 && parts[1]?.trim() && parts[2]?.trim()) {
        const questionText = `${parts[1].trim()} ____${parts[2].slice(-1)}`; // Keep ending punctuation
        const answerText = parts[2].slice(0, -1).trim(); // Answer is the second part, trimmed
        if (!answerText) return null; // Avoid empty answers
        return { id: crypto.randomUUID(), question: questionText, answer: answerText, source: sourceInfo, topic: "补全下一句 (2分)" };
    }
    // Fallback: Use the next line as the answer if two lines are available
    if (lines.length >= 2 && lines[0]?.trim() && lines[1]?.trim()) {
        // Remove common ending punctuation from the answer line
        const answerText = lines[1].replace(/[。？！]$/, '').trim();
        if (!answerText) return null;
        return { id: crypto.randomUUID(), question: `${lines[0]} ____。`, answer: answerText, source: sourceInfo, topic: "补全下一句 (2分)" };
    }
    return null;
};

// Pattern: Fill in the first part of a sentence (e.g., "____，DEF。")
const patternFillPrevious: QuestionPattern = (lines, sourceInfo) => {
    if (lines.length < 1) return null;
    const line = lines[0];
    const parts = line.match(/^(.*?[，、；])(.*?[。？！])$/);
    if (parts && parts.length === 3 && parts[1]?.trim() && parts[2]?.trim()) {
        const questionText = `____${parts[1].slice(-1)} ${parts[2].trim()}`; // Keep punctuation separator
        const answerText = parts[1].slice(0, -1).trim();
        if (!answerText) return null;
        return { id: crypto.randomUUID(), question: questionText, answer: answerText, source: sourceInfo, topic: "补全上一句 (2分)" };
    }
     // Fallback: Use the first line as the answer if two lines available
    if (lines.length >= 2 && lines[0]?.trim() && lines[1]?.trim()) {
        const answerText = lines[0].replace(/[。？！]$/, '').trim();
        if (!answerText) return null;
        return { id: crypto.randomUUID(), question: `____， ${lines[1]}`, answer: answerText, source: sourceInfo, topic: "补全上一句 (2分)" };
    }
    return null;
};

// Add more patterns here if needed (e.g., write two consecutive lines)
// const patternWriteTwoConsecutive: QuestionPattern = ...

// Array of available patterns to randomly choose from
const availablePatterns: QuestionPattern[] = [
    patternFillNext,
    patternFillPrevious,
    // patternWriteTwoConsecutive, // Add more complex patterns if implemented
];

// Function to generate a set of questions using the patterns and flattened data
function generateQuestionSetFromFanwei(fanwei: KaoshiFanweiItem[], count: number): QuestionInfo[] {
    const generatedQuestions: QuestionInfo[] = [];
    // Use a Set to track used *sourceItem.id* to avoid picking the same poem/article twice in one set
    const usedSourceItemIds = new Set<string>();
    let attempts = 0;
    const maxAttempts = count * 30; // Increase attempts for more complex selection

    while (generatedQuestions.length < count && attempts < maxAttempts) {
        attempts++;
        if (fanwei.length === 0) {
             console.warn("Cannot generate more questions, fanwei data is empty.");
             break;
        }

        // Pick a random source item (poem/article)
        const randomSourceIndex = Math.floor(Math.random() * fanwei.length);
        const sourceItem = fanwei[randomSourceIndex];

        // Skip if this source item was already used in this set
        if (!sourceItem || usedSourceItemIds.has(sourceItem.id)) {
             continue;
        }

        // Find a suitable starting line index within the content
        // Ensure there are enough lines following the start index for the pattern
        if (sourceItem.content.length < MIN_LINES_FOR_QUESTION) {
            // This check should be redundant due to flattening logic, but good failsafe
            console.warn(`Skipping source item ${sourceItem.id} during generation due to insufficient lines: ${sourceItem.content.length}`);
            usedSourceItemIds.add(sourceItem.id); // Mark as used even if skipped due to length
            continue;
        }
        const randomLineIndex = Math.floor(Math.random() * (sourceItem.content.length - (MIN_LINES_FOR_QUESTION - 1)));
        const selectedLines = sourceItem.content.slice(randomLineIndex, randomLineIndex + MIN_LINES_FOR_QUESTION);

        // Randomly select a pattern to apply
        const randomPattern = availablePatterns[Math.floor(Math.random() * availablePatterns.length)];

        // Construct source information string
        const sourceInfo = `${sourceItem.category} - 《${sourceItem.title}》${sourceItem.author ? ` - ${sourceItem.author}` : ''}`;

        // Try to generate a question with the selected lines and pattern
        const newQuestion = randomPattern(selectedLines, sourceInfo);

        if (newQuestion) {
             // Add check to avoid duplicate *answers* within the same set, if desired
            if (!generatedQuestions.some(q => q.answer === newQuestion.answer)) {
                 generatedQuestions.push(newQuestion);
                 // Mark this source item ID as used for this set
                 usedSourceItemIds.add(sourceItem.id);
            } else {
                 console.log(`Skipping generated question due to duplicate answer: ${newQuestion.answer}`);
            }
        }
    }

     // Log if fewer questions were generated than requested
     if (generatedQuestions.length < count) {
          console.warn(`Could only generate ${generatedQuestions.length} unique questions out of ${count} requested.`);
     }
    return generatedQuestions;
}

// --- Gemini API Call Function ---

async function callGeminiAPI(apiKey: string, model: string, contents: GeminiContent[], generationConfig?: { maxOutputTokens?: number; temperature?: number; }): Promise<GeminiApiResponse> {
    const url = `${GEMINI_API_BASE_URL}${model}:generateContent?key=${apiKey}`;
    console.log(`Calling Gemini API: ${url} with model ${model}`);
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ contents, generationConfig }),
        });
        console.log(`Gemini API response status: ${response.status}`);

        // Handle non-OK responses
        if (!response.ok) {
            const errorBody = await response.text();
            console.error(`Gemini API Error Response Body:`, errorBody);
            // Attempt to parse Google's structured error message
            let detailMessage = `AI API Error (${response.status}): ${response.statusText}`;
            try {
                 const googleError = JSON.parse(errorBody);
                 detailMessage = googleError?.error?.message || detailMessage; // Use detailed message if available
            } catch(e) { /* Ignore JSON parsing error, use original status text */ }
            throw new Error(detailMessage); // Throw with potentially more informative message
        }

        // Handle OK responses that might not be JSON
        const contentType = response.headers.get("content-type");
        if (contentType && contentType.includes("application/json")) {
            return await response.json() as GeminiApiResponse; // Parse as JSON
        } else {
             const textResponse = await response.text();
             console.error("Gemini API returned non-JSON response:", textResponse);
             throw new Error("AI API returned unexpected response format (non-JSON).");
        }
    } catch (error: any) {
        console.error("Network or other error calling Gemini API:", error);
        // Propagate a generic but informative error
        throw new Error(`Failed to communicate with AI service: ${error.message || 'Unknown network error'}`);
    }
}

// --- Main Request Handler ---
export const onRequest: PagesFunction<Env> = async (context) => {
    // Destructure context for easier access
    const { request, env, params } = context;
    const url = new URL(request.url);
    // Assuming functions/api/[[path]].ts structure for path segments
    const apiPath = (params.path as string[] || []).join('/');

    // Standard headers for CORS and JSON response
    const baseHeaders = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Access-Control-Allow-Origin': '*', // Adjust for production environments
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization', // Include other headers if needed
    };

    // Handle CORS Preflight requests
    if (request.method === 'OPTIONS') {
        return new Response(null, { headers: baseHeaders });
    }

    console.log(`[${new Date().toISOString()}] Request: ${request.method} /api/${apiPath}`);

    // --- Environment Check ---
    // Fail early if essential configurations are missing
    if (!env.IMAGES_BUCKET || !env.GEMINI_API_KEY || !env.SESSION_KV) {
         console.error("FATAL: Server configuration error: Missing required Cloudflare bindings (R2, KV) or secrets (GEMINI_API_KEY). Check project settings.");
         return new Response(JSON.stringify({ error: "Server configuration error. Please contact administrator." }), { status: 500, headers: baseHeaders });
    }

    // --- Data Preparation ---
    // Flatten the imported raw data. Cache this result if the data is large and static.
    let kaoshifanweiData: KaoshiFanweiItem[];
    try {
        // Use 'unknown' cast for robustness if TS inference on JSON import is unreliable
        kaoshifanweiData = flattenKaoshiFanweiData(kaoshifanweiRawData as unknown as KaoshiFanweiRawStructure);
    } catch (e: any) {
        console.error("Failed to process kaoshifanwei data:", e);
        return new Response(JSON.stringify({ error: "內部數據處理錯誤。"}), { status: 500, headers: baseHeaders });
    }

    try {
        // --- API Routing ---

        // Simple health check endpoint
        if (apiPath === 'hello' && request.method === 'GET') {
            const dataInfo = {
                message: "墨力全開 Backend is running.",
                status: "OK",
                timestamp: new Date().toISOString(),
                kaoshifanweiItemsLoaded: kaoshifanweiData.length, // Report count from flattened data
                zhentiItemsLoaded: Array.isArray(zhentiData) ? zhentiData.length : 0, // Basic check if zhenti is array
            };
            return new Response(JSON.stringify(dataInfo), { headers: baseHeaders });
        }

        // API Endpoint to start a new question set
        if (apiPath === 'start_set' && request.method === 'GET') {
             console.log("Processing /api/start_set request");
             if (kaoshifanweiData.length === 0) {
                  console.error("Cannot generate questions: Flattened kaoshifanwei data is empty.");
                  throw new Error("處理後的考試範圍數據為空，無法出題。");
             }

            const setId = crypto.randomUUID();
            // Generate questions using the flattened data
            const generatedQuestions = generateQuestionSetFromFanwei(kaoshifanweiData, MAX_QUESTIONS_PER_SET);

            // Handle cases where not enough unique questions could be generated
            if (generatedQuestions.length < MAX_QUESTIONS_PER_SET) {
                 console.warn(`Warning: Only generated ${generatedQuestions.length} questions for setId ${setId}. Returning partial set.`);
                 // Decide: Allow partial sets or throw error? Let's allow partial for now.
            }
            if (generatedQuestions.length === 0) {
                 console.error("Failed to generate any questions from flattened fanwei data.");
                 throw new Error("無法生成任何題目，請檢查數據源或生成邏輯。");
            }

            // Create the QuestionSet object to store in KV (includes answers)
            const newSet: QuestionSet = {
                setId: setId,
                questions: generatedQuestions,
                createdAt: Date.now()
            };

            // Store the set in Cloudflare KV with an expiration time
            try {
                await env.SESSION_KV.put(setId, JSON.stringify(newSet), { expirationTtl: KV_EXPIRATION_TTL_SECONDS });
                console.log(`Stored new question set in KV with setId: ${setId} (${generatedQuestions.length} questions)`);
            } catch (kvError: any) {
                console.error(`KV put error for setId ${setId}:`, kvError);
                throw new Error(`無法保存題組信息: ${kvError.message}`); // Propagate error
            }

            // Prepare and return the response for the frontend (without answers)
            // Explicitly type the parameters in map callback
            const questionsForFrontend = newSet.questions.map(({ answer, ...rest }: QuestionInfo) => rest);
            return new Response(JSON.stringify({ setId: newSet.setId, questions: questionsForFrontend }), { headers: baseHeaders });
        } // End /api/start_set


        // API Endpoint to submit answers (image upload)
        if (apiPath === 'submit' && request.method === 'POST') {
            console.log("Processing /api/submit request");

            // --- Request Parsing and Validation ---
            const formData = await request.formData();
            const setIdValue = formData.get('setId'); // Type: FormDataEntryValue | null
            const imageValue = formData.get('handwritingImage'); // Type: FormDataEntryValue | null
            let imageFile: File; // Declare variable to hold the validated File

            // 1. Validate setId
            if (typeof setIdValue !== 'string' || !setIdValue) {
                 console.error("Invalid submit request: Missing or invalid setId.", { setIdValue });
                 return new Response(JSON.stringify({ error: '請求無效：缺少有效的題組 ID。' }), { status: 400, headers: baseHeaders });
            }
            const setId: string = setIdValue; // Assign validated setId

            // 2. Validate imageValue presence (handles null)
            if (!imageValue) {
                console.error("Invalid submit request: Missing imageValue.", { setId });
                return new Response(JSON.stringify({ error: '請求無效：未上傳圖片。' }), { status: 400, headers: baseHeaders });
            }
             // ---> At this point, imageValue is: string | File

            // 3. Check if imageValue is a string
            if (typeof imageValue === 'string') {
                // If it's a string, it's invalid
                console.error("Invalid submit request: Uploaded value is a string, expected File.", {
                    setId,
                    valuePreview: imageValue.substring(0, 100) + "..."
                });
                return new Response(JSON.stringify({ error: '請求錯誤：上傳的數據格式不正確（應為文件）。' }), { status: 400, headers: baseHeaders });
            }
            // ---> Logically, imageValue must be File here. Let's assert it.

            // --- Use Type Assertion ---
            // Explicitly tell TypeScript that imageValue is a File at this point.
            const tempImageFile = imageValue as File;

            // 4. Check File size using the asserted variable
            if (tempImageFile.size === 0) {
                 console.error("Invalid submit request: Uploaded File is empty.", {
                     setId,
                     // Access properties from the asserted variable
                     fileName: tempImageFile.name,
                     size: tempImageFile.size
                 });
                 return new Response(JSON.stringify({ error: '請求無效：上傳的圖片文件大小為 0。' }), { status: 400, headers: baseHeaders });
            }

            // --- Validation Passed ---
            // Assign the asserted and validated file to the main variable
            imageFile = tempImageFile;
            console.log(`Validation passed for setId: ${setId}. Image: ${imageFile.name}, Size: ${imageFile.size}, Type: ${imageFile.type}`);
            // `imageFile` now safely holds the validated File object


            // --- Retrieve Question Set from KV ---
            let questionSet: QuestionSet | null = null;
            try {
                questionSet = await env.SESSION_KV.get<QuestionSet>(setId, 'json');
            } catch (kvError: any) {
                console.error(`KV get error for setId ${setId}:`, kvError);
                return new Response(JSON.stringify({ error: "無法獲取題組信息，會話可能已過期或ID無效，請重新開始挑戰。" }), { status: 404, headers: baseHeaders });
            }
            // Check if set exists and has questions
            if (!questionSet || !questionSet.questions || questionSet.questions.length === 0 ) {
                console.error(`Invalid or missing/empty question set data in KV for setId ${setId}`, questionSet);
                return new Response(JSON.stringify({ error: "無效的題組信息，請重新開始挑戰。" }), { status: 400, headers: baseHeaders });
            }
            // Determine the actual number of questions in this set
            const expectedQuestionCount = questionSet.questions.length;
            console.log(`Found ${expectedQuestionCount} questions for setId ${setId}.`);

            // Extract correct answers and question IDs
            const correctAnswers = questionSet.questions.map((q: QuestionInfo) => q.answer);
            const questionIds = questionSet.questions.map((q: QuestionInfo) => q.id);
            console.log(`Retrieved correct answers for setId ${setId}`);


            // --- Store Image to R2 ---
            const imageBuffer = await imageFile.arrayBuffer(); // Use the validated imageFile
            const r2Key = generateUniqueKey(`set-${setId}-answer`, `.${imageFile.type.split('/')[1] || 'png'}`);
            try {
                 await env.IMAGES_BUCKET.put(r2Key, imageBuffer, { httpMetadata: { contentType: imageFile.type }});
                 console.log(`Stored image in R2 with key: ${r2Key} for setId: ${setId}`);
            } catch (r2Error: any) {
                 console.error(`R2 put error for key ${r2Key}:`, r2Error);
                 // Consider if the flow should continue if R2 fails, or throw
                 throw new Error(`圖片存儲失敗: ${r2Error.message || 'Unknown R2 error'}`);
            }


            // --- Call Gemini Vision for OCR ---
            const base64ImageData = arrayBufferToBase64(imageBuffer); // Use buffer from validated imageFile
            const ocrStartTime = Date.now();
            let recognizedTextCombined = '';
            let ocrError: string | null = null;
            let splitAnswers: string[] = [];
            // Construct the OCR prompt dynamically based on the expected number of questions
            const ocrContents: GeminiContent[] = [{
                parts: [
                    { "text": `这是一张包含${expectedQuestionCount}个手写简体中文答案的图片，按从上到下的顺序排列。请准确识别每个答案，并只用换行符（\\n）分隔返回${expectedQuestionCount}个结果。不要添加任何其他文字、解释、编号或格式。如果某个答案无法识别，请在那一行输出 "[無法識別]"。` },
                    { "inline_data": { "mime_type": imageFile.type || "image/png", "data": base64ImageData } } // Use type from validated imageFile
                ]
            }];

            try {
                const geminiResult = await callGeminiAPI(env.GEMINI_API_KEY, GEMINI_VISION_MODEL, ocrContents, { maxOutputTokens: 800, temperature: 0.1 });
                const ocrDuration = Date.now() - ocrStartTime;
                console.log(`Gemini OCR completed for setId ${setId} in ${ocrDuration}ms.`);

                const candidate = geminiResult.candidates?.[0];
                const part = candidate?.content?.parts?.[0];
                // Safely access text part
                if (part && 'text' in part) {
                    recognizedTextCombined = part.text.trim();
                } else {
                    ocrError = "AI返回了非預期的響應格式。";
                    console.warn(`OCR Result format issue for setId ${setId}. Part:`, part);
                }

                // Process recognized text if no format error and text is not empty
                if (!ocrError && !recognizedTextCombined) {
                    ocrError = "AI未能識別出任何文本內容。";
                    console.warn(`OCR Result empty for setId ${setId}`);
                } else if (!ocrError) {
                    console.log(`Raw OCR result for setId ${setId}: "${recognizedTextCombined.replace(/\n/g, '\\n')}"`);
                    // Split by newline, trim, and filter out fully empty lines
                    splitAnswers = recognizedTextCombined.split('\n').map(s => s.trim()).filter(s => s);

                    // Compare split count with the actual number of questions expected for this set
                    if (splitAnswers.length !== expectedQuestionCount) {
                        console.warn(`OCR split count mismatch for setId ${setId}: expected ${expectedQuestionCount}, got ${splitAnswers.length}.`);
                        ocrError = `AI未能準確分割出 ${expectedQuestionCount} 個答案 (找到了 ${splitAnswers.length} 個)。`;
                        // Pad or truncate to match the expected number for the results array structure
                        while (splitAnswers.length < expectedQuestionCount) splitAnswers.push("[答案提取失敗]");
                        if (splitAnswers.length > expectedQuestionCount) splitAnswers = splitAnswers.slice(0, expectedQuestionCount);
                    } else {
                        console.log(`Successfully split OCR into ${splitAnswers.length} answers for setId ${setId}.`);
                    }
                }
            } catch (err: any) {
                 console.error(`Gemini OCR API call failed for setId ${setId}:`, err);
                 ocrError = `AI 識別服務調用失敗: ${err.message}`;
                 // Ensure splitAnswers array has the correct length for the scoring loop, filled with error placeholders
                 splitAnswers = Array(expectedQuestionCount).fill(`[AI調用失敗]`);
            }


            // --- Scoring ---
            const results: SubmissionResult[] = [];
            let totalScore = 0;
            // Calculate points per question based on the target score (8) and actual number of questions
            const pointsPerQuestion = expectedQuestionCount > 0 ? (8 / expectedQuestionCount) : 0;

            // Iterate based on the actual number of questions retrieved from KV
            for (let i = 0; i < expectedQuestionCount; i++) {
                const recognized = splitAnswers[i] || "[答案缺失]"; // Fallback
                const correct = correctAnswers[i];
                const questionId = questionIds[i];
                let isCorrect = false;
                let score = 0;
                // Determine success based on OCR error and specific recognition status
                let success = !ocrError || (recognized !== "[答案提取失敗]" && recognized !== "[AI調用失敗]");

                if (success && correct !== undefined) {
                    isCorrect = recognized === correct; // Strict comparison
                    // Handle specific case where AI explicitly couldn't recognize
                    if (recognized === "[無法識別]") {
                        isCorrect = false;
                        success = false; // Mark as unsuccessful if AI couldn't recognize
                    }
                    // Assign score dynamically
                    score = isCorrect ? pointsPerQuestion : 0;
                } else if (!success && !ocrError) {
                    // If no general OCR error, likely due to padding or '[無法識別]'
                    ocrError = ocrError || "部分答案未能成功識別或提取。";
                }

                results.push({
                     questionIndex: i,
                     questionId: questionId,
                     success: success,
                     recognizedText: recognized,
                     correctAnswer: correct || "[標準答案缺失]",
                     isCorrect: isCorrect,
                     score: score,
                     // Include placeholder error text if success is false
                     error: !success && recognized.startsWith("[") ? recognized : undefined
                });
                totalScore += score;
            }
            // Round total score to one decimal place for consistency
            totalScore = Math.round(totalScore * 10) / 10;
            console.log(`Scoring complete for setId ${setId}. Total score: ${totalScore} / 8`);


            // --- Generate Feedback using AI ---
            let feedback = "";
            const feedbackStartTime = Date.now();
            // Check score against the target of 8
            if (totalScore === 8) {
                 feedback = `滿分 8 分！不錯，繼續保持這種狀態！`;
            } else {
                // Prepare details for the feedback prompt
                const incorrectResults = results.filter(r => !r.isCorrect);
                // Explicitly type 'r' in map callback
                const errorDetails = incorrectResults
                    .map((r: SubmissionResult) => `第 ${r.questionIndex + 1} 題:\n  你的答案: "${r.recognizedText}"\n  正確答案: "${r.correctAnswer}"`)
                    .join('\n\n');

                // Construct the prompt for the "angry teacher" AI
                const feedbackPrompt = `你是一位極其嚴厲、對學生要求極高的高考語文老師，你對學生在默寫環節丟分感到極度憤怒和失望。以下是這位學生的默寫結果，請根據錯誤詳情，生成一段充滿斥罵口吻、強調高考殘酷性、指出問題嚴重性、並命令學生立刻改正的訓斥反饋。語氣要非常嚴厲，不留情面！

                學生總分: ${totalScore.toFixed(1)} / 8
                錯誤題目詳情:
                ${errorDetails || "雖然未滿分，但未能定位具體錯誤文本（可能是 AI 識別問題）。"}
                ${ocrError ? `\n系統提示: ${ocrError}` : ''}

                請直接生成訓斥文字，不要加任何開頭或結尾的客套話。`;

                try {
                    console.log(`Generating AI feedback for setId ${setId}...`);
                    const feedbackContents: GeminiContent[] = [{ parts: [{ text: feedbackPrompt }] }];
                    const feedbackResult = await callGeminiAPI(env.GEMINI_API_KEY, GEMINI_TEXT_MODEL, feedbackContents, { maxOutputTokens: 500, temperature: 0.7 });

                    // Safely access the text part of the feedback response
                    const feedbackPart = feedbackResult.candidates?.[0]?.content?.parts?.[0];
                    if (feedbackPart && 'text' in feedbackPart) {
                        feedback = feedbackPart.text.trim();
                        const feedbackDuration = Date.now() - feedbackStartTime;
                        console.log(`AI feedback generated successfully for setId ${setId} in ${feedbackDuration}ms.`);
                    } else {
                         console.warn(`AI feedback generation returned no text/unexpected format for setId ${setId}. Falling back.`);
                         // Append feedback error to existing ocrError if any
                         ocrError = (ocrError ? ocrError + "; " : "") + "AI反饋生成異常。";
                         feedback = `總分 ${totalScore.toFixed(1)}！錯了 ${ (8 - totalScore).toFixed(1)} 分！自己好好反省！\n${errorDetails || '(無法生成詳細反饋)'}`; // Fallback feedback
                    }
                } catch (feedbackError: any) {
                    console.error(`Gemini feedback generation failed for setId ${setId}:`, feedbackError);
                    ocrError = (ocrError ? ocrError + "; " : "") + `AI反饋生成失敗: ${feedbackError.message}`;
                    feedback = `總分 ${totalScore.toFixed(1)}！錯了 ${(8 - totalScore).toFixed(1)} 分！問題嚴重！回去好好反思！\n${errorDetails || '(無法生成詳細反饋)'}`; // Fallback feedback on error
                }
            }

            // --- Prepare Final Response ---
            const responseData = {
                message: ocrError ? "評分完成，但 AI 處理過程遇到問題。" : "評分完成。",
                totalScore: totalScore, // Return the potentially fractional score, frontend can format
                results: results,
                feedback: feedback,
                r2Key: r2Key, // Include R2 key for potential debugging
                ocrIssue: ocrError // Pass any accumulated error messages
            };
            // Return the response to the client
            return new Response(JSON.stringify(responseData), { headers: baseHeaders });
        } // End /api/submit


        // --- Fallback for unmatched API routes ---
        console.log(`API route not found: /api/${apiPath}`);
        return new Response(JSON.stringify({ error: `API route /api/${apiPath} not found` }), { status: 404, headers: baseHeaders });

    } catch (error: any) {
        // --- General Error Handling for the entire request ---
        console.error(`Unhandled error processing /api/${apiPath}:`, error);
        // Determine appropriate status code (use error's status if available and valid, else 500)
        const status = (typeof error.status === 'number' && error.status >= 400 && error.status < 600) ? error.status : 500;
        // Avoid exposing sensitive internal error details for 5xx errors
        const errorMessage = (status < 500 && error.message) ? error.message : '伺服器內部錯誤，請稍後再試。'; // Show message for client errors
        // Log the full error stack internally for debugging 5xx errors
        if (status >= 500) {
            console.error(`Responding with Internal Server Error (${status}). Stack:`, error.stack);
        }
        return new Response(JSON.stringify({ error: errorMessage }), { status: status, headers: baseHeaders });
    }
}; // End onRequest Handler