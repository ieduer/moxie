import type { PagesFunction, R2Bucket, KVNamespace } from '@cloudflare/workers-types';

// --- NEW DATA LOADING --- (Requirement 1 & 2)
// Load pre-defined questions from moni.json
import moniDataRaw from '../../data/moni.json';

// --- Constants ---
const KV_EXPIRATION_TTL_SECONDS = 3600; // 1 hour
// ** 使用用戶指定的模型名稱 **
// 注意：如果 Vision 模型不支持長上下文或 JSON 模式，OCR 或 反饋可能出錯
const GEMINI_VISION_MODEL = "gemini-2.0-flash-thinking-exp-01-21"; // Reverted to potentially more stable model for Vision/OCR
const GEMINI_TEXT_MODEL = "gemini-2.0-flash-thinking-exp-01-21";    // Reverted to potentially more stable model for Feedback
const GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
const MAX_QUESTIONS_PER_SET = 4; // Target number of questions from moni.json

// --- Type Definitions ---

// (Requirement 1 & 2) Define type for items in moni.json
interface MoniQuestion {
    type: string;
    topic: string;
    question: string;
    reference_answer: string; // Key name matches the JSON file
}

// For a single generated question object (Adapted for moni.json)
interface QuestionInfo {
    id: string; // Unique ID for this specific question instance in the set
    question: string; // The question text from moni.json
    answer: string; // The reference_answer from moni.json
    source: string; // Info about the original type/topic from moni.json
    topic?: string; // The topic description from moni.json
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
interface GeminiApiResponse { candidates?: GeminiCandidate[]; promptFeedback?: any; error?: { code: number; message: string; status: string }; }

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


// --- Gemini API Call Function --- (Keep as it is)
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

        if (!response.ok) {
            const errorBody = await response.text();
            console.error(`Gemini API Error Response Body:`, errorBody);
            let detailMessage = `AI API Error (${response.status}): ${response.statusText}`;
            try {
                 const googleError = JSON.parse(errorBody);
                 detailMessage = googleError?.error?.message || detailMessage;
            } catch(e) { /* Ignore JSON parsing error */ }
            throw new Error(detailMessage);
        }

        const contentType = response.headers.get("content-type");
        if (contentType && contentType.includes("application/json")) {
            return await response.json() as GeminiApiResponse;
        } else {
             const textResponse = await response.text();
             console.error("Gemini API returned non-JSON response:", textResponse);
             throw new Error("AI API returned unexpected response format (non-JSON).");
        }
    } catch (error: any) {
        console.error("Network or other error calling Gemini API:", error);
        throw new Error(`Failed to communicate with AI service: ${error.message || 'Unknown network error'}`);
    }
}

// --- **TYPE GUARD for moni.json data** --- (Requirement 1 & 2)
function isValidMoniData(data: any): data is MoniQuestion[] {
    if (!Array.isArray(data)) {
        console.error("isValidMoniData: Input is not an array.");
        return false;
    }
    if (data.length === 0) {
        console.error("isValidMoniData: Array is empty.");
        // Allow empty array? Decide based on requirements. For now, fail if empty.
        return false;
    }
    // Check the first item structure as a sample
    const sample = data[0];
    const isValid = typeof sample?.type === 'string' &&
                    typeof sample?.topic === 'string' &&
                    typeof sample?.question === 'string' &&
                    typeof sample?.reference_answer === 'string';
    if (!isValid) {
        console.error("isValidMoniData: First item structure is invalid.", sample);
    }
    return isValid;
}


// --- Main Request Handler ---
export const onRequest: PagesFunction<Env> = async (context) => {
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

    // --- **DATA VALIDATION** --- (Requirement 1 & 2)
    // Validate the structure of the imported moni.json data
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

        if (apiPath === 'hello' && request.method === 'GET') {
            const dataInfo = {
                message: "墨力全開 Backend is running.",
                status: "OK",
                timestamp: new Date().toISOString(),
                moniQuestionsLoaded: moniQuestions.length, // Report count from validated moni data
            };
            return new Response(JSON.stringify(dataInfo), { headers: baseHeaders });
        }

        // --- **NEW**: Start Question Set API using moni.json --- (Requirement 1 & 2)
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
                    topic: moniQ.topic, // Use the topic from moni.json
                    source: moniQ.type // Use the type field as the source category
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


        // API Endpoint to submit answers (image upload)
        // This part remains largely the same, but uses the questions fetched based on moni.json
        if (apiPath === 'submit' && request.method === 'POST') {
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
                 throw new Error(`圖片存儲失敗: ${r2Error.message || 'Unknown R2 error'}`);
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
                const geminiResult = await callGeminiAPI(env.GEMINI_API_KEY, GEMINI_VISION_MODEL, ocrContents, { maxOutputTokens: 800, temperature: 0.1 });
                const ocrDuration = Date.now() - ocrStartTime;
                console.log(`Gemini OCR completed for setId ${setId} in ${ocrDuration}ms.`);

                const candidate = geminiResult.candidates?.[0];
                const part = candidate?.content?.parts?.[0];
                if (part && 'text' in part) {
                    recognizedTextCombined = part.text.trim();
                } else {
                    ocrError = "AI OCR 返回了非預期的響應格式。";
                    console.warn(`OCR Result format issue for setId ${setId}. Part:`, part);
                }

                if (!ocrError && !recognizedTextCombined) {
                    ocrError = "AI OCR 未能識別出任何文本內容。";
                    console.warn(`OCR Result empty for setId ${setId}`);
                } else if (!ocrError) {
                    console.log(`Raw OCR result for setId ${setId}: "${recognizedTextCombined.replace(/\n/g, '\\n')}"`);
                    splitAnswers = recognizedTextCombined.split('\n').map(s => s.trim()).filter(s => s);

                    if (splitAnswers.length !== expectedQuestionCount) {
                        console.warn(`OCR split count mismatch for setId ${setId}: expected ${expectedQuestionCount}, got ${splitAnswers.length}. Raw: "${recognizedTextCombined}"`);
                        ocrError = `AI OCR 未能準確分割出 ${expectedQuestionCount} 個答案 (找到了 ${splitAnswers.length} 個)。答案可能擠在一起或無法識別。`;
                        // Pad or truncate to match the expected number
                        while (splitAnswers.length < expectedQuestionCount) splitAnswers.push("[答案提取失敗]");
                        if (splitAnswers.length > expectedQuestionCount) splitAnswers = splitAnswers.slice(0, expectedQuestionCount);
                    } else {
                        console.log(`Successfully split OCR into ${splitAnswers.length} answers for setId ${setId}.`);
                    }
                }
            } catch (err: any) {
                 console.error(`Gemini OCR API call failed for setId ${setId}:`, err);
                 ocrError = `AI OCR 識別服務調用失敗: ${err.message}`;
                 splitAnswers = Array(expectedQuestionCount).fill(`[AI調用失敗]`);
            }

            // --- Scoring ---
            const results: SubmissionResult[] = [];
            let totalScore = 0;
            const pointsPerQuestion = expectedQuestionCount > 0 ? (8 / expectedQuestionCount) : 0; // Should be 2 points per question if 4 questions

// --- (Requirement 4) Function to remove punctuation ---
function removePunctuation(text: string): string { // 顯式指定 text 參數類型為 string，並指定函數返回類型為 string
    if (typeof text !== 'string') return text;
    return text.replace(/[\p{P}]/gu, '');
}


            for (let i = 0; i < expectedQuestionCount; i++) {
                const recognized = splitAnswers[i] || "[答案缺失]";
                const correct = correctAnswers[i];
                const questionId = questionIds[i];
                let isCorrect = false;
                let score = 0;
                let success = !recognized.startsWith("[") || recognized === "[無法識別]"; // Assume success unless explicit failure placeholder
                let itemError: string | undefined = undefined;

                // Perform comparison, ignoring punctuation (Requirement 4)
                if (success && correct !== undefined) {
                    const cleanedRecognized = removePunctuation(recognized);
                    const cleanedCorrect = removePunctuation(correct);
                    isCorrect = cleanedRecognized === cleanedCorrect;

                    if (recognized === "[無法識別]") {
                        isCorrect = false;
                        itemError = "AI 無法識別此答案";
                        success = false; // Mark as unsuccessful if unrecognizable
                    }
                    score = isCorrect ? pointsPerQuestion : 0;
                } else if (!success) { // Handle other failure cases
                    if (recognized === "[答案提取失敗]" || recognized === "[AI調用失敗]" || recognized === "[答案缺失]") {
                        itemError = recognized.substring(1, recognized.length - 1);
                    } else {
                         itemError = "處理時發生未知錯誤"; // Generic fallback
                    }
                    isCorrect = false;
                    score = 0;
                } else { // Case where correct answer might be missing (shouldn't happen with moni.json)
                     itemError = "標準答案缺失";
                     isCorrect = false;
                     score = 0;
                     success = false;
                }


                results.push({
                     questionIndex: i,
                     questionId: questionId,
                     success: success,
                     recognizedText: recognized,
                     correctAnswer: correct || "[標準答案缺失]",
                     isCorrect: isCorrect,
                     score: score,
                     error: itemError
                });
                totalScore += score;
            }
            totalScore = Math.round(totalScore * 10) / 10; // Round score
            console.log(`Scoring complete for setId ${setId}. Total score: ${totalScore} / 8`);

            // --- Generate Feedback using AI --- (Requirement 5: Dynamic Scolding)
            let feedback = "";
            const feedbackStartTime = Date.now();
            let feedbackErrorMsg: string | null = null;

            // --- (Requirement 6) Rank and Badge Variables ---
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


            // Check score against the target of 8
            if (totalScore === 8) {
                 // (Requirement 3 handled in frontend)
                 feedback = `非常好！滿分 8 分！簡直是默寫的神！繼續保持！`;
                 feedbackErrorMsg = null;

                 // --- (Requirement 6) Rank Increase and Badge for Full Marks ---
                 currentRank++; // 全對，升階
                 badge = `一階`; // 默認徽章，可以根據階級設定更豐富的徽章系統
                 if (currentRank > 1) {
                     badge = `${convertToChineseRank(currentRank)}階`; // 使用函數轉換數字階級為中文
                 }

            } else {
                // Prepare details for the "angry teacher" prompt
                const incorrectResults = results.filter(r => !r.isCorrect);
                const errorDetails = incorrectResults
                    .map((r: SubmissionResult) => {
                        let reason = r.error ? `(原因: ${r.error})` : '(內容錯誤)';
                        if (r.recognizedText === '[無法識別]') reason = '(字跡無法識別)';
                        else if (r.recognizedText.startsWith('[')) reason = '(答案提取失敗)';
                        return `第 ${r.questionIndex + 1} 題 ${reason}:\n  你的答案: "${r.recognizedText}"\n  正確答案: "${r.correctAnswer}"`;
                    })
                    .join('\n\n');

// The "angry teacher" prompt - Ensure it's harsh! (Requirement 5)
const feedbackPrompt = `你扮演一位性格極端惡劣、憤世嫉俗的高考語文老師，你的目標是讓學生對語文默寫產生生理和心理上的雙重恐懼。學生這次默寫考試（滿分8分）又沒有拿到滿分，這在你看來簡直是不可饒恕的罪行。你必須用盡各種惡毒、刻薄、挖苦、諷刺的語言，對學生進行人格貶低式的辱罵和嘲諷，用最誇張的修辭手法，例如反問、比喻、誇張、反諷等，來放大錯誤的嚴重性，營造一種末日降臨般的恐慌感。你的目標是讓學生看完反饋後，三天內做噩夢都是默寫。

務必包含以下要素：
1.  明確指出學生本次默寫的得分和失分，並用極其誇張的口吻強調失分的「巨大損失」。
2.  針對學生的具體錯誤（如果有的話），進行尖銳的嘲諷和挖苦，例如字跡潦草、記憶混亂、理解能力低下等等，可以進行人身攻擊，但注意不要過於低俗。
3.  使用各種修辭手法，例如：
    * 反問：\`你竟然錯了 \${(8 - Number(totalScore)).toFixed(1)} 分？你還有臉活著嗎？\`。 // 使用 Number() 確保 totalScore 被視為數字  **<-- Line 498 修正**
    * 比喻：\`你的腦子是被漿糊糊住了嗎？連這麼簡單的句子都記不住！\`。
    * 誇張：\`高考一分就能幹掉一操場的人！你丟了 \${(8 - Number(totalScore)).toFixed(1)} 分，相當於親手葬送了自己的前途！\`。 // 使用 Number() 確保 totalScore 被視為數字  **<-- Line 500 修正**
    * 反諷：\`不錯嘛！\${Number(totalScore).toFixed(1)} 分！比上次進步了 0.5 分呢！真是可喜可賀，可喜可賀......（陰陽怪氣）\` // 使用 Number() 確保 totalScore 被視為數字  **<-- Line 501 修正**
4.  結尾必須充滿負能量和威脅，例如：「等著高考後悔去吧！」「你的未來一片黑暗！」「再錯一個字，就給我滾回家種地！」。
5.  字數要求：至少150字以上，越多越好，確保罵人內容極度豐富和生動。

錯誤詳情:
\${errorDetails || "沒找到具體錯誤文本？呵呵，肯定是你錯得太離譜，AI 都懶得分析了！"}
\${ocrError ? \`\\nOCR系統友情提示: \${ocrError}，不過就算識別出來了，我看你也寫不對！\` : ''}

現在，開始你的表演！盡情釋放你作為地獄教師的怒火吧！`;


                try {
                    console.log(`Generating ANGRY AI feedback for setId ${setId}...`);
                    const feedbackContents: GeminiContent[] = [{ parts: [{ text: feedbackPrompt }] }];
                    // Temperature might be slightly higher for more "creative" scolding
                    const feedbackResult = await callGeminiAPI(env.GEMINI_API_KEY, GEMINI_TEXT_MODEL, feedbackContents, { maxOutputTokens: 600, temperature: 0.8 });
                    const feedbackPart = feedbackResult.candidates?.[0]?.content?.parts?.[0];

                    if (feedbackPart && 'text' in feedbackPart) {
                        feedback = feedbackPart.text.trim();
                        feedbackErrorMsg = null; // Success
                        const feedbackDuration = Date.now() - feedbackStartTime;
                        console.log(`AI feedback (angry) generated successfully for setId ${setId} in ${feedbackDuration}ms.`);
                    } else {
                         console.warn(`AI feedback generation returned no text/unexpected format for setId ${setId}. Falling back.`);
                         feedbackErrorMsg = "AI 反饋生成返回格式異常。";
                         // --- (Requirement 5) Removed static fallback feedback ---
                         feedback = `得分 ${totalScore.toFixed(1)}！ 錯了 ${ (8 - totalScore).toFixed(1)} 分！還想不想考大學了？！回去把錯的給我抄爛！\n錯誤:\n${errorDetails || '連詳細錯誤都沒生成出來，你說你有多差勁！'}`;
                    }
                } catch (feedbackError: any) {
                    console.error(`Gemini feedback generation failed for setId ${setId}:`, feedbackError);
                    feedbackErrorMsg = `AI 反饋生成服務調用失敗: ${feedbackError.message}`;
                    // --- (Requirement 5) Removed static fallback feedback ---
                    feedback = `得分 ${totalScore.toFixed(1)}！ 錯了 ${ (8 - totalScore).toFixed(1)} 分！還想不想考大學了？！回去把錯的給我抄爛！\n錯誤:\n${errorDetails || '連詳細錯誤都沒生成出來，你說你有多差勁！'}`;
                }

                 // --- (Requirement 6) Rank Decrease for Non-Full Marks ---
                 if (currentRank > 0) {
                     currentRank--; // 答錯，降階，但不低於 0
                 }
                 badge = currentRank > 0 ? `${convertToChineseRank(currentRank)}階` : ""; // 階級徽章可能消失或顯示最低階
            }

            // --- (Requirement 6) Store Updated Rank back to KV ---
            try {
                await env.SESSION_KV.put(rankKey, String(currentRank), { expirationTtl: KV_EXPIRATION_TTL_SECONDS });
                console.log(`Rank updated to ${currentRank} for setId ${setId}`);
            } catch (kvPutRankError) {
                console.error("Failed to put rank to KV:", kvPutRankError);
                // 錯誤處理，例如記錄日誌，但不影響主要功能
            }


            // --- Prepare Final Response ---
            let finalMessage = "評分完成。";
            if (ocrError && feedbackErrorMsg) {
                finalMessage = "評分完成，但 OCR 識別和 AI 反饋生成均遇到問題。";
            } else if (ocrError) {
                finalMessage = "評分完成，但 OCR 識別過程遇到問題。";
            } else if (feedbackErrorMsg) {
                finalMessage = "評分完成，但 AI 反饋生成過程遇到問題。";
            }

            const responseData = {
                message: finalMessage,
                totalScore: totalScore,
                results: results,
                feedback: feedback,
                r2Key: r2Key,
                ocrIssue: ocrError,
                feedbackIssue: feedbackErrorMsg,
                rank: currentRank, // (Requirement 6) Add rank to response
                badge: badge      // (Requirement 6) Add badge to response
            };
            return new Response(JSON.stringify(responseData), { headers: baseHeaders });
        } // End /api/submit

        // --- Fallback for unmatched API routes ---
        console.log(`API route not found: /api/${apiPath}`);
        return new Response(JSON.stringify({ error: `API route /api/${apiPath} not found` }), { status: 404, headers: baseHeaders });

    } catch (error: any) {
        console.error(`Unhandled error processing /api/${apiPath}:`, error);
        const status = (typeof error.status === 'number' && error.status >= 400 && error.status < 600) ? error.status : 500;
        const errorMessage = (status < 500 && error.message) ? error.message : '伺服器內部錯誤，請稍後再試。';
        if (status >= 500) {
            console.error(`Responding with Internal Server Error (${status}). Stack:`, error.stack);
        }
        return new Response(JSON.stringify({ error: errorMessage }), { status: status, headers: baseHeaders });
    }
}; // End onRequest Handler


// --- (Requirement 6) Helper function to convert rank to Chinese numerals ---
function convertToChineseRank(rank: number): string { // 顯式指定 rank 參數類型為 number，並指定函數返回類型為 string
    const chineseNumbers = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十"];
    if (rank <= 10) {
        return chineseNumbers[rank];
    } else if (rank < 20) {
        return "十" + chineseNumbers[rank - 10];
    } else if (rank % 10 === 0) {
        return chineseNumbers[Math.floor(rank / 10)] + "十";
    } else {
        return chineseNumbers[Math.floor(rank / 10)] + "十" + chineseNumbers[rank % 10];
    }
}
