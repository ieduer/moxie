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

// 生成隨機文件名 (例如 UUID)
function generateUniqueKey(prefix = 'upload', extension = '.png'): string {
     // 簡單的基於時間戳和隨機數的方法，生產環境建議用 UUID 庫
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
    // R2 存儲桶綁定
    IMAGES_BUCKET: R2Bucket;

    // 未來添加 Gemini API Key (從 Secrets 獲取)
    // GEMINI_API_KEY: string;
}

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
            // ... (hello 接口邏輯保持不變) ...
             const dataInfo = {
                message: '墨力全開後端 Function 已啟動！',
                status: 'OK',
                kaoshiFanweiLoaded: kaoshifanwei ? `Loaded ${kaoshifanwei.文言文?.length || 0} 文言文, ${kaoshifanwei.诗词曲?.length || 0} 詩詞曲` : 'Failed',
                zhentiLoaded: zhenti ? `Loaded ${zhenti?.length || 0} 真題 items` : 'Failed',
                r2BindingStatus: env.IMAGES_BUCKET ? 'R2 Bucket Bound' : 'R2 Bucket NOT Bound', // 檢查 R2 綁定
                timestamp: new Date().toISOString()
            };
            if (!env.IMAGES_BUCKET) console.error("R2 Bucket 'IMAGES_BUCKET' is not bound in the environment!");
            if (!kaoshifanwei || !zhenti || zhenti.length === 0) {
                console.warn("Warning: Data files might not be loaded correctly.");
                dataInfo.status = "Warning: Data load issue detected.";
            }
            return new Response(JSON.stringify(dataInfo), { headers });
        }

        if (apiPath === 'question' && request.method === 'GET') {
            // ... (question 接口邏輯保持不變) ...
            if (!kaoshifanwei || !zhenti || zhenti.length === 0) {
                console.error("Error: Prerequisite data not available.");
                throw new Error("後端數據準備失敗，無法生成題目。");
            }
            const numberOfQuestions = 4;
            const randomZhentiItems = getRandomItems(zhenti, numberOfQuestions);
            const questions = randomZhentiItems.map((item: any, index: number) => {
                 let source = "未知來源";
                 if (item.reference_answer && kaoshifanwei) {
                     const foundSource = kaoshifanwei.文言文?.find(p => p.content?.includes(item.reference_answer))?.title ||
                                       kaoshifanwei.诗词曲?.find(p => p.content?.includes(item.reference_answer))?.title;
                     if (foundSource) source = foundSource;
                 }
                return {
                    id: item.id || index + 1, // 優先使用真題中的 ID（如果有的話）
                    topic: item.topic || "在横线处填写作品原句。(共2分)",
                    question: item.question || "題目描述缺失",
                    reference_answer: item.reference_answer || "答案缺失", // 後端保留
                    source: source
                };
            });
             // --- 後端存儲答案以備評分 (非常重要！) ---
             // 在實際應用中，你需要將生成的題目（特別是 ID 和 reference_answer）
             // 存儲起來（例如使用 KV 或 D1），與一個唯一的會話 ID 關聯。
             // 這裡暫時只在內存中模擬，每次請求都會重新生成。

            const questionsForFrontend = questions.map(({ reference_answer, ...rest }) => rest);
            return new Response(JSON.stringify(questionsForFrontend), { headers });
        }

        if (apiPath === 'submit' && request.method === 'POST') {
             // 檢查 R2 綁定是否存在
            if (!env.IMAGES_BUCKET) {
                console.error("R2 Bucket 'IMAGES_BUCKET' is not configured/bound.");
                return new Response(JSON.stringify({ error: 'Server configuration error: Image storage is unavailable.' }), { status: 500, headers });
            }

            const payload = await request.json<{ answers: { questionId: number | string; imageDataUrl: string | null }[] }>();
            if (!payload || !Array.isArray(payload.answers)) {
                return new Response(JSON.stringify({ error: 'Invalid request body format.' }), { status: 400, headers });
            }

            const uploadResults = [];
            const processingErrors = [];

            for (const answer of payload.answers) {
                if (answer.imageDataUrl) {
                    const imageBuffer = dataUrlToBlob(answer.imageDataUrl);
                    if (imageBuffer) {
                        const key = generateUniqueKey(`q-${answer.questionId}`);
                        try {
                            // 上傳到 R2
                            const startTime = Date.now();
                            await env.IMAGES_BUCKET.put(key, imageBuffer, {
                                httpMetadata: { contentType: 'image/png' }, // 指定 Content-Type
                            });
                            const duration = Date.now() - startTime;
                            console.log(`Uploaded ${key} to R2 in ${duration}ms`);
                            uploadResults.push({ questionId: answer.questionId, success: true, r2Key: key });
                            // TODO: 在這裡或之後調用 OCR
                        } catch (err: any) {
                            console.error(`Failed to upload image for question ${answer.questionId} to R2:`, err);
                            processingErrors.push({ questionId: answer.questionId, error: `Image upload failed: ${err.message}` });
                            uploadResults.push({ questionId: answer.questionId, success: false, error: err.message });
                        }
                    } else {
                        console.error(`Failed to decode base64 image for question ${answer.questionId}`);
                        processingErrors.push({ questionId: answer.questionId, error: 'Image data decoding failed.' });
                         uploadResults.push({ questionId: answer.questionId, success: false, error: 'Decoding failed' });
                    }
                } else {
                    // 處理空白答案
                     uploadResults.push({ questionId: answer.questionId, success: true, r2Key: null, message: "Blank answer" });
                     // TODO: 空白答案直接判 0 分
                }
            }

            // TODO: 根據 uploadResults 中的 r2Key 調用 OCR，然後進行評分和生成反饋

            // 暫時返回上傳結果
            const responseData = {
                message: "Answers received and processed.",
                results: uploadResults,
                errors: processingErrors.length > 0 ? processingErrors : undefined
                // TODO: 添加評分結果、解析、斥罵等
            };

            return new Response(JSON.stringify(responseData), { headers });
        }

        // --- 未匹配的路由 ---
        return new Response(JSON.stringify({ error: `API route /api/${apiPath} not found or method ${request.method} not allowed.` }), { status: 404, headers });

    } catch (error: any) {
        console.error(`Error processing /api/${apiPath}:`, error);
        return new Response(JSON.stringify({ error: error.message || 'Internal Server Error' }), { status: 500, headers });
    }
};