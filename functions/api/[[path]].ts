import type { PagesFunction, R2Bucket, KVNamespace } from '@cloudflare/workers-types';

// 靜態導入 JSON 文件
import kaoshifanwei from '../../data/kaoshifanwei.json';
import zhenti from '../../data/zhenti.json';

// --- 輔助函數 ---
function arrayBufferToBase64(buffer: ArrayBuffer): string {
    let binary = '';
    const bytes = new Uint8Array(buffer);
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) { binary += String.fromCharCode(bytes[i]); }
    return btoa(binary);
}

function generateUniqueKey(prefix = 'upload', extension = '.png'): string {
     return `${prefix}-${Date.now()}-${Math.random().toString(36).substring(2, 9)}${extension}`;
}

function getRandomItems<T>(arr: T[], num: number): T[] {
    if (!Array.isArray(arr)) { console.error("getRandomItems: input is not an array", arr); return []; }
    const shuffled = [...arr].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, num);
}

// --- 環境變量和綁定接口 ---
interface Env {
    IMAGES_BUCKET: R2Bucket;
    GEMINI_API_KEY: string;
    SESSION_KV: KVNamespace; // **添加 KV 綁定**
}

// --- Gemini API 響應類型接口 (保持不變) ---
interface GeminiTextPart { text: string; }
interface GeminiContent { parts: GeminiTextPart[]; role?: string; }
interface GeminiCandidate { content: GeminiContent; finishReason?: string; index?: number; safetyRatings?: any[]; }
interface GeminiApiResponse { candidates?: GeminiCandidate[]; promptFeedback?: any; error?: { code: number; message: string; status: string }; }

// --- 題組數據結構 (用於 KV 存儲) ---
interface QuestionInfo {
    id: string | number;
    question: string;
    answer: string;
    source: string;
    topic?: string;
}
interface QuestionSet {
    setId: string;
    questions: QuestionInfo[];
    userId?: string; // 可選，用於追蹤用戶
    createdAt: number;
}

// --- 主處理函數 ---
export const onRequest: PagesFunction<Env> = async (context) => {
    const { request, env, params } = context;
    const url = new URL(request.url);
    const pathSegments = params.path as string[];
    const apiPath = pathSegments.join('/');

    console.log(`[${new Date().toISOString()}] Request: ${request.method} /api/${apiPath}`);

    if (request.method === 'OPTIONS') { /* ... CORS Preflight ... */ }

    const headers = { 'Content-Type': 'application/json;charset=UTF-8', 'Access-Control-Allow-Origin': '*' };

    try {
        // --- API 路由 ---
        if (apiPath === 'hello' && request.method === 'GET') {
             const dataInfo = { /* ... (可添加 KV 綁定狀態檢查) ... */ };
             if (!env.SESSION_KV) console.error("KV Namespace 'SESSION_KV' is not bound!");
             /* ... 其他檢查 ... */
             return new Response(JSON.stringify(dataInfo), { headers });
        }

        // **TODO: 實現 /api/start_set 接口**
        // if (apiPath === 'start_set' && request.method === 'GET') { ... }

        // **暫時保留 /api/question 作為獲取題目接口 (未來應廢棄)**
        if (apiPath === 'question' && request.method === 'GET') {
            if (!zhenti || zhenti.length === 0) throw new Error("真題數據加載失敗");
            const numberOfQuestions = 4;
            const randomZhentiItems = getRandomItems(zhenti, numberOfQuestions);
            // 注意：此處未與 KV 交互，setId 也是前端生成的
            const questionsForFrontend = randomZhentiItems.map((item: any, index: number) => ({
                 id: item.id || `gen-${index + 1}`,
                 topic: item.topic || "在横线处填写作品原句。(共2分)",
                 question: item.question || "題目描述缺失",
                 // 不返回答案
            }));
            return new Response(JSON.stringify(questionsForFrontend), { headers });
        }


        // --- **修改 /api/submit 接口** ---
        if (apiPath === 'submit' && request.method === 'POST') {
            if (!env.IMAGES_BUCKET || !env.GEMINI_API_KEY || !env.SESSION_KV) {
                throw new Error("Server configuration error: Missing required bindings or secrets.");
            }

            const formData = await request.formData();
            const setId = formData.get('setId') as string;
            const imageFile = formData.get('handwritingImage') as File;

            if (!setId || !imageFile || !(imageFile instanceof File)) {
                throw new Error('Invalid request: Missing setId or handwritingImage.');
            }

             // **TODO: 從 KV 中獲取該 setId 的正確答案**
             // let questionSet: QuestionSet | null = null;
             // try {
             //     questionSet = await env.SESSION_KV.get<QuestionSet>(setId, 'json');
             // } catch (kvError) {
             //     console.error(`KV get error for setId ${setId}:`, kvError);
             //     throw new Error("無法獲取題組信息，請重新開始。");
             // }
             // if (!questionSet || !questionSet.questions || questionSet.questions.length !== 4) {
             //      console.error(`Invalid or missing question set data in KV for setId ${setId}`);
             //      throw new Error("無效的題組信息，請重新開始。");
             // }
             // const correctAnswers = questionSet.questions.map(q => q.answer);
             // **--- 臨時方案：使用 zhenti 數據模擬正確答案 (假設 setId 與題目順序有關) ---**
             // **這部分邏輯非常不可靠，必須用 KV 替換！**
             const tempCorrectAnswers = zhenti.slice(0, 4).map((item: any) => item.reference_answer || "");
             if (tempCorrectAnswers.length !== 4) throw new Error("臨時答案數據錯誤");
             const correctAnswers = tempCorrectAnswers;
             // **--- 臨時方案結束 ---**


            // 1. 上傳圖片到 R2
            const imageBuffer = await imageFile.arrayBuffer();
            const key = generateUniqueKey(setId, '.png'); // 用 setId 作為前綴
            try {
                await env.IMAGES_BUCKET.put(key, imageBuffer, { httpMetadata: { contentType: imageFile.type } });
                console.log(`Uploaded ${key} to R2 for setId: ${setId}`);
            } catch (r2Error: any) {
                 console.error(`R2 put error for key ${key}:`, r2Error);
                 throw new Error(`圖片存儲失敗: ${r2Error.message}`);
            }

            // 2. 調用 Gemini OCR
            const base64ImageData = arrayBufferToBase64(imageBuffer);
            const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro-latest:generateContent?key=${env.GEMINI_API_KEY}`;
            const ocrStartTime = Date.now();
            let recognizedTextCombined = '';
            let ocrError = null;

            try {
                const geminiResponse = await fetch(geminiUrl, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        contents: [{ parts: [
                            // **新的 Prompt，要求換行分隔**
                            { "text": "这是一张包含4个手写简体中文答案的图片，按从上到下的顺序排列。请准确识别每个答案，并用换行符（\\n）分隔返回4个结果。不要添加任何其他文字或格式。" },
                            { "inline_data": { "mime_type": imageFile.type || "image/png", "data": base64ImageData } }
                        ] }],
                        generationConfig: { "maxOutputTokens": 400, "temperature": 0.1 } // 增加 token 預期
                    })
                });
                const ocrDuration = Date.now() - ocrStartTime;
                console.log(`Gemini response received for setId ${setId} in ${ocrDuration}ms. Status: ${geminiResponse.status}`);

                if (!geminiResponse.ok) { /* ... 處理 API 錯誤 ... */ throw new Error(`AI OCR failed: ${geminiResponse.statusText}`);}

                const geminiResult = await geminiResponse.json() as GeminiApiResponse;
                const candidate = geminiResult.candidates?.[0];
                const part = candidate?.content?.parts?.[0];
                recognizedTextCombined = part?.text?.trim() || '';

            } catch (err: any) {
                 console.error(`Gemini API call failed for setId ${setId}:`, err);
                 ocrError = `AI 服務調用失敗: ${err.message}`;
            }

            // 3. 分割答案並評分
            const results = [];
            let totalScore = 0;
            let splitAnswers = [];

            if (!ocrError && recognizedTextCombined) {
                 // 按換行符分割
                 splitAnswers = recognizedTextCombined.split('\n').map(s => s.trim()).filter(s => s); // 過濾空行
                 if (splitAnswers.length !== 4) {
                     console.warn(`OCR split result count mismatch for setId ${setId}: expected 4, got ${splitAnswers.length}. Combined text: "${recognizedTextCombined}"`);
                     // 嘗試填充或標記錯誤，這裡簡單填充空字符串
                     while (splitAnswers.length < 4) splitAnswers.push("[答案提取失敗]");
                     if (splitAnswers.length > 4) splitAnswers = splitAnswers.slice(0, 4); // 截斷
                     ocrError = "AI 未能準確分割 4 個答案"; // 設置錯誤信息
                 }
            } else if (!ocrError) {
                ocrError = "AI 未返回有效識別結果";
            }

            // 進行評分
            for (let i = 0; i < 4; i++) {
                 const recognized = splitAnswers[i] || (ocrError ? `[${ocrError}]` : "[未能識別]");
                 const correct = correctAnswers[i];
                 let isCorrect = false;
                 let score = 0;
                 let success = !ocrError || i < splitAnswers.length; // 認為處理成功除非有 OCR 錯誤且無法分割

                 if (success && correct !== undefined) {
                     isCorrect = recognized === correct;
                     score = isCorrect ? 2 : 0;
                 }

                 results.push({
                     questionIndex: i, // 或使用 questionSet.questions[i].id
                     success: success,
                     recognizedText: recognized,
                     correctAnswer: correct,
                     isCorrect: isCorrect,
                     score: score,
                     error: i >= splitAnswers.length ? ocrError : undefined // 如果填充了錯誤信息
                 });
                 totalScore += score;
            }

            // 4. 生成總結性反饋 (未來實現 AI 生成)
             let feedback = "";
             if (totalScore === 8) {
                 feedback = "不錯，拿到滿分了！繼續保持！";
             } else {
                  // TODO: 調用 Gemini 生成帶斥罵的 feedback
                  feedback = `總分 ${totalScore}！離滿分差 ${8 - totalScore} 分！高考默寫一分都不能丟！回去好好練！`;
                  if(ocrError) feedback += ` (AI 提示: ${ocrError})`; // 附加 OCR 錯誤
             }

            const responseData = {
                message: ocrError ? "評分完成（部分識別可能有問題）。" : "評分完成。",
                totalScore: totalScore,
                results: results,
                feedback: feedback, // 添加反饋字段
                r2Key: key // 返回存儲的 key 以便調試
            };

            // **TODO: 更新 KV 狀態** (例如記錄分數，完成狀態等)
            // await env.SESSION_KV.put(...);

            return new Response(JSON.stringify(responseData), { headers });
        }

        // --- 未匹配的路由 ---
        return new Response(JSON.stringify({ error: `API route /api/${apiPath} not found` }), { status: 404, headers });

    } catch (error: any) {
        console.error(`Error processing /api/${apiPath}:`, error);
        const errorMessage = (error.message.includes('configuration error') || error.message.includes('Invalid request')) ? error.message : 'Internal Server Error';
        return new Response(JSON.stringify({ error: errorMessage }), { status: 500, headers });
    }
};