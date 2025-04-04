import type { PagesFunction, R2Bucket } from '@cloudflare/workers-types';

// 靜態導入 JSON 文件
import kaoshifanwei from '../../data/kaoshifanwei.json';
import zhenti from '../../data/zhenti.json';

// --- 輔助函數 ---

// Base64 Data URL 解碼為 ArrayBuffer
function dataUrlToBlob(dataUrl: string): ArrayBuffer | null {
    if (!dataUrl || !dataUrl.startsWith('data:image/png;base64,')) {
        console.error("Invalid data URL format");
        return null;
    }
    const base64 = dataUrl.split(',')[1];
    if (!base64) return null;
    try {
        const binaryString = atob(base64);
        const len = binaryString.length;
        const bytes = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            bytes[i] = binaryString.charCodeAt(i);
        }
        return bytes.buffer;
    } catch (e) {
        console.error("Error decoding base64 string:", e);
        return null;
    }
}

// Base64 編碼 ArrayBuffer
function arrayBufferToBase64(buffer: ArrayBuffer): string {
    let binary = '';
    const bytes = new Uint8Array(buffer);
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}


// 生成隨機文件名
function generateUniqueKey(prefix = 'upload', extension = '.png'): string {
     return `${prefix}-${Date.now()}-${Math.random().toString(36).substring(2, 9)}${extension}`;
}

// 從數組中隨機選取指定數量的元素
function getRandomItems<T>(arr: T[], num: number): T[] {
    if (!Array.isArray(arr)) {
        console.error("getRandomItems: input is not an array", arr);
        return [];
    }
    const shuffled = [...arr].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, num);
}

// --- 環境變量和綁定接口 ---
interface Env {
    IMAGES_BUCKET: R2Bucket;
    GEMINI_API_KEY: string;
}

// --- Gemini API 響應類型接口 ---
// (根據實際響應調整，這是一個可能的結構)
interface GeminiTextPart {
    text: string;
}
interface GeminiContent {
    parts: GeminiTextPart[];
    role?: string;
}
interface GeminiCandidate {
    content: GeminiContent;
    finishReason?: string;
    index?: number;
    safetyRatings?: any[]; // 內容可能更複雜
}
interface GeminiApiResponse {
    candidates?: GeminiCandidate[]; // 設為可選，以防 API 返回錯誤時沒有 candidates
    promptFeedback?: any;
    // 可能有 error 字段
    error?: { code: number; message: string; status: string };
}


// --- 全局變量存儲當前題目答案 (警告：僅限演示) ---
let currentReferenceAnswers: { [key: string]: string } = {};

// --- 主處理函數 ---
export const onRequest: PagesFunction<Env> = async (context) => {
    const { request, env, params } = context;
    const url = new URL(request.url);
    const pathSegments = params.path as string[];
    const apiPath = pathSegments.join('/');

    console.log(`[${new Date().toISOString()}] Request: ${request.method} /api/${apiPath}`);

    if (request.method === 'OPTIONS') {
         return new Response(null, {
            headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' },
        });
    }

    const headers = { 'Content-Type': 'application/json;charset=UTF-8', 'Access-Control-Allow-Origin': '*' };

    try {
        // --- API 路由 ---
        if (apiPath === 'hello' && request.method === 'GET') {
             const dataInfo = {
                message: '墨力全開後端 Function 已啟動！',
                status: 'OK',
                kaoshiFanweiLoaded: kaoshifanwei ? `Loaded ${kaoshifanwei.文言文?.length || 0} 文言文, ${kaoshifanwei.诗词曲?.length || 0} 詩詞曲` : 'Failed',
                zhentiLoaded: zhenti ? `Loaded ${zhenti?.length || 0} 真題 items` : 'Failed',
                r2BindingStatus: env.IMAGES_BUCKET ? 'R2 Bucket Bound' : 'R2 Bucket NOT Bound',
                geminiKeyStatus: env.GEMINI_API_KEY ? 'Gemini Key Set' : 'Gemini Key NOT Set',
                timestamp: new Date().toISOString()
            };
            if (!env.IMAGES_BUCKET) console.error("R2 Bucket 'IMAGES_BUCKET' is not bound!");
            if (!env.GEMINI_API_KEY) console.error("Secret 'GEMINI_API_KEY' is not set!");
            if (!kaoshifanwei || !zhenti || zhenti.length === 0) {
                console.warn("Warning: Data files might not be loaded correctly.");
                dataInfo.status = "Warning: Data load issue detected.";
            }
            return new Response(JSON.stringify(dataInfo), { headers });
        }

        if (apiPath === 'question' && request.method === 'GET') {
            if (!kaoshifanwei || !zhenti || zhenti.length === 0) throw new Error("後端數據準備失敗");

            const numberOfQuestions = 4;
            const randomZhentiItems = getRandomItems(zhenti, numberOfQuestions);
            currentReferenceAnswers = {}; // 清空

            const questions = randomZhentiItems.map((item: any, index: number) => {
                 const questionId = item.id || `gen-${index + 1}`;
                 const referenceAnswer = item.reference_answer || "";
                 currentReferenceAnswers[questionId] = referenceAnswer;

                 let source = "未知來源";
                 // ... (查找來源邏輯) ...

                return {
                    id: questionId,
                    topic: item.topic || "在横线处填写作品原句。(共2分)",
                    question: item.question || "題目描述缺失",
                    source: source
                };
            });

            console.log("Stored reference answers for this set:", Object.keys(currentReferenceAnswers));
            return new Response(JSON.stringify(questions), { headers });
        }

        if (apiPath === 'submit' && request.method === 'POST') {
            if (!env.IMAGES_BUCKET) throw new Error("Server configuration error: Image storage is unavailable.");
            if (!env.GEMINI_API_KEY) throw new Error("Server configuration error: AI service key is missing.");

            const payload = await request.json<{ answers: { questionId: number | string; imageDataUrl: string | null }[] }>();
            if (!payload || !Array.isArray(payload.answers)) throw new Error('Invalid request body format.');

            const results = []; // 存儲結果

            // --- 使用 for...of 循環串行處理 ---
            for (const answer of payload.answers) {
                const questionId = answer.questionId;
                const correctAnswer = currentReferenceAnswers[questionId];

                let currentResult: any = {
                    questionId: questionId, success: false, recognizedText: null,
                    isCorrect: false, score: 0,
                    correctAnswer: correctAnswer ?? "[未找到標準答案]"
                };

                if (correctAnswer === undefined) {
                    console.error(`Cannot find reference answer for questionId: ${questionId}`);
                    currentResult.error = "無法評分：未找到標準答案";
                }

                if (answer.imageDataUrl) {
                    const imageBuffer = dataUrlToBlob(answer.imageDataUrl);
                    if (imageBuffer) {
                        const key = generateUniqueKey(`q-${questionId}`);
                        try {
                            await env.IMAGES_BUCKET.put(key, imageBuffer, { httpMetadata: { contentType: 'image/png' } });
                            console.log(`Uploaded ${key} to R2 for qid: ${questionId}`);
                            currentResult.r2Key = key;

                            const base64ImageData = arrayBufferToBase64(imageBuffer);
                            const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro-latest:generateContent?key=${env.GEMINI_API_KEY}`;

                            const ocrStartTime = Date.now();
                            console.log(`[${ocrStartTime}] Calling Gemini for qid: ${questionId}...`);

                            const geminiResponse = await fetch(geminiUrl, {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    contents: [{ parts: [
                                        { "text": "请识别这张图片中的所有手写简体中文文字，并直接返回文本结果，不要包含任何 markdown 格式或描述性文字。" },
                                        { "inline_data": { "mime_type": "image/png", "data": base64ImageData } }
                                    ] }],
                                     generationConfig: { "maxOutputTokens": 150, "temperature": 0.1 }
                                })
                            });
                            const ocrDuration = Date.now() - ocrStartTime;
                             console.log(`[${Date.now()}] Gemini response received for qid: ${questionId} in ${ocrDuration}ms. Status: ${geminiResponse.status}`);

                            // 在解析 JSON 之前檢查狀態碼
                            if (!geminiResponse.ok) {
                                const errorText = await geminiResponse.text(); // 嘗試讀取錯誤文本
                                let errorMessage = `AI OCR failed: ${geminiResponse.statusText}`;
                                if (geminiResponse.status === 429) errorMessage = `AI OCR failed: Too Many Requests (速率限制)`;
                                else if (geminiResponse.status === 400) errorMessage = `AI OCR failed: Bad Request`;
                                else if (geminiResponse.status === 500) errorMessage = `AI OCR failed: Internal Server Error`;
                                console.error(`Gemini API error for qid ${questionId}: ${geminiResponse.status} ${errorMessage}`, errorText);
                                throw new Error(errorMessage); // 拋出錯誤
                            }

                            // *** 進行類型斷言 ***
                            const geminiResult = await geminiResponse.json() as GeminiApiResponse;

                            // 提取識別文本 (進行更安全的訪問)
                            try {
                                // ** 安全訪問 **
                                const candidate = geminiResult.candidates?.[0];
                                const part = candidate?.content?.parts?.[0];
                                const recognizedTextRaw = part?.text;

                                if (recognizedTextRaw) {
                                    currentResult.recognizedText = recognizedTextRaw.trim().replace(/\s+/g, '');
                                } else {
                                     // 如果結構不對或沒有文本，記錄錯誤
                                     console.error(`Unexpected Gemini response structure or missing text for qid ${questionId}:`, JSON.stringify(geminiResult));
                                     currentResult.recognizedText = "[OCR 無有效結果]";
                                     currentResult.error = "OCR 無有效結果";
                                }
                            } catch (e) {
                                console.error(`Error parsing Gemini response for qid ${questionId}:`, e, JSON.stringify(geminiResult));
                                currentResult.recognizedText = "[OCR 解析錯誤]";
                                currentResult.error = "OCR 解析錯誤";
                            }

                            // 比對和評分
                            if (correctAnswer !== undefined && !currentResult.error && currentResult.recognizedText) {
                                if (currentResult.recognizedText === correctAnswer) {
                                    currentResult.isCorrect = true;
                                    currentResult.score = 2;
                                } else {
                                    currentResult.isCorrect = false;
                                    currentResult.score = 0;
                                }
                            } else {
                                currentResult.isCorrect = false;
                                currentResult.score = 0;
                            }
                            currentResult.success = true;

                        } catch (err: any) { // 捕獲上傳或 OCR 過程中的異常
                            console.error(`Processing failed for qid ${questionId}:`, err);
                            currentResult.error = err.message || '未知處理錯誤';
                            currentResult.success = false;
                            currentResult.score = 0;
                            currentResult.isCorrect = false;
                        }
                    } else { // 解碼失敗
                         currentResult.error = 'Image data decoding failed.';
                         currentResult.success = false;
                         currentResult.score = 0;
                         currentResult.isCorrect = false;
                    }
                } else { // 空白答案
                    currentResult.success = true;
                    currentResult.r2Key = null;
                    currentResult.recognizedText = "[空白]";
                    currentResult.isCorrect = false;
                    currentResult.score = 0;
                }
                results.push(currentResult);
                // await new Promise(resolve => setTimeout(resolve, 200)); // 可選延遲
            }

            const totalScore = results.reduce((sum, r) => sum + (r.score || 0), 0);

            const responseData = {
                message: "評分完成。",
                totalScore: totalScore,
                results: results,
            };

            return new Response(JSON.stringify(responseData), { headers });
        }

        // --- 未匹配的路由 ---
        return new Response(JSON.stringify({ error: `API route /api/${apiPath} not found` }), { status: 404, headers });

    } catch (error: any) {
        console.error(`Error processing /api/${apiPath}:`, error);
        const errorMessage = (error.message.includes('configuration error') || error.message.includes('Invalid request'))
                             ? error.message : 'Internal Server Error';
        return new Response(JSON.stringify({ error: errorMessage }), { status: 500, headers });
    }
};