import type { PagesFunction, R2Bucket } from '@cloudflare/workers-types';

// 靜態導入 JSON 文件
import kaoshifanwei from '../../data/kaoshifanwei.json';
import zhenti from '../../data/zhenti.json';

// --- 輔助函數 ---

// Base64 Data URL 解碼為 ArrayBuffer (保持不變)
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

// Base64 編碼 ArrayBuffer (用於 Gemini API)
function arrayBufferToBase64(buffer: ArrayBuffer): string {
    let binary = '';
    const bytes = new Uint8Array(buffer);
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}


// 生成隨機文件名 (保持不變)
function generateUniqueKey(prefix = 'upload', extension = '.png'): string {
     return `${prefix}-${Date.now()}-${Math.random().toString(36).substring(2, 9)}${extension}`;
}

// 從數組中隨機選取指定數量的元素 (保持不變)
function getRandomItems<T>(arr: T[], num: number): T[] {
    // ... (代碼不變) ...
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
    // 添加 Gemini API Key (從 Secrets 讀取)
    GEMINI_API_KEY: string;
}

// --- 全局變量存儲當前題目答案 (警告：僅限演示，生產環境不可靠) ---
// 這是一個臨時方案，因為 Pages Functions 本身是無狀態的。
// 理想情況下，`referenceAnswers` 應該與用戶會話關聯並存儲在 KV 或 D1 中。
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
            // ... (hello 接口邏輯保持不變，可以添加 GEMINI_API_KEY 狀態檢查) ...
             const dataInfo = {
                message: '墨力全開後端 Function 已啟動！',
                status: 'OK',
                kaoshiFanweiLoaded: kaoshifanwei ? `Loaded ${kaoshifanwei.文言文?.length || 0} 文言文, ${kaoshifanwei.诗词曲?.length || 0} 詩詞曲` : 'Failed',
                zhentiLoaded: zhenti ? `Loaded ${zhenti?.length || 0} 真題 items` : 'Failed',
                r2BindingStatus: env.IMAGES_BUCKET ? 'R2 Bucket Bound' : 'R2 Bucket NOT Bound',
                geminiKeyStatus: env.GEMINI_API_KEY ? 'Gemini Key Set' : 'Gemini Key NOT Set', // 檢查 Key
                timestamp: new Date().toISOString()
            };
            if (!env.IMAGES_BUCKET) console.error("R2 Bucket 'IMAGES_BUCKET' is not bound!");
            if (!env.GEMINI_API_KEY) console.error("Secret 'GEMINI_API_KEY' is not set!");
            // ... (其他檢查) ...
            return new Response(JSON.stringify(dataInfo), { headers });
        }

        if (apiPath === 'question' && request.method === 'GET') {
            if (!kaoshifanwei || !zhenti || zhenti.length === 0) throw new Error("後端數據準備失敗");

            const numberOfQuestions = 4;
            const randomZhentiItems = getRandomItems(zhenti, numberOfQuestions);
            currentReferenceAnswers = {}; // 清空上一輪的答案

            const questions = randomZhentiItems.map((item: any, index: number) => {
                 let source = "未知來源";
                 const questionId = item.id || `gen-${index + 1}`; // 生成唯一 ID
                 const referenceAnswer = item.reference_answer || ""; // 獲取答案

                 if (referenceAnswer && kaoshifanwei) { /* ... (查找來源邏輯) ... */ }

                 // **臨時存儲正確答案到全局變量** (關聯 ID)
                 currentReferenceAnswers[questionId] = referenceAnswer;

                return {
                    id: questionId, // 使用生成的 ID
                    topic: item.topic || "在横线处填写作品原句。(共2分)",
                    question: item.question || "題目描述缺失",
                    // reference_answer: referenceAnswer, // 不再返回給前端
                    source: source
                };
            });

            console.log("Stored reference answers for this set:", Object.keys(currentReferenceAnswers)); // 調試日誌

            // 只返回題目給前端
            return new Response(JSON.stringify(questions), { headers });
        }

        if (apiPath === 'submit' && request.method === 'POST') {
             // 檢查 R2 和 API Key
            if (!env.IMAGES_BUCKET) throw new Error("Server configuration error: Image storage is unavailable.");
            if (!env.GEMINI_API_KEY) throw new Error("Server configuration error: AI service key is missing.");

            const payload = await request.json<{ answers: { questionId: number | string; imageDataUrl: string | null }[] }>();
            if (!payload || !Array.isArray(payload.answers)) throw new Error('Invalid request body format.');

            // --- 使用 Promise.all 並行處理所有答案 ---
            const processingPromises = payload.answers.map(async (answer) => {
                const questionId = answer.questionId;
                const correctAnswer = currentReferenceAnswers[questionId]; // 從臨時存儲獲取答案

                if (correctAnswer === undefined) {
                    console.error(`Cannot find reference answer for questionId: ${questionId}`);
                    // 即使找不到答案，也繼續處理圖片，但標記錯誤
                }

                if (answer.imageDataUrl) {
                    const imageBuffer = dataUrlToBlob(answer.imageDataUrl);
                    if (imageBuffer) {
                        const key = generateUniqueKey(`q-${questionId}`);
                        try {
                            // 1. 上傳 R2
                            await env.IMAGES_BUCKET.put(key, imageBuffer, { httpMetadata: { contentType: 'image/png' } });
                            console.log(`Uploaded ${key} to R2 for qid: ${questionId}`);

                            // 2. 調用 Gemini OCR
                            const base64ImageData = arrayBufferToBase64(imageBuffer);
                            const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro-latest:generateContent?key=${env.GEMINI_API_KEY}`;
                            // const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-pro-vision:generateContent?key=${env.GEMINI_API_KEY}`; // 或者用 pro-vision

                            const ocrStartTime = Date.now();
                            const geminiResponse = await fetch(geminiUrl, {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    contents: [{
                                        parts: [
                                            { "text": "请识别这张图片中的所有手写简体中文文字，并直接返回文本结果，不要包含任何 markdown 格式或描述性文字。" },
                                            { "inline_data": { "mime_type": "image/png", "data": base64ImageData } }
                                        ]
                                    }],
                                    // 可選配置，例如限制輸出
                                     generationConfig: {
                                        // "candidateCount": 1,
                                        "maxOutputTokens": 150, // 根據答案長度調整
                                        "temperature": 0.1, // 降低隨機性
                                    }
                                })
                            });
                            const ocrDuration = Date.now() - ocrStartTime;

                            if (!geminiResponse.ok) {
                                const errorText = await geminiResponse.text();
                                console.error(`Gemini API error for qid ${questionId}: ${geminiResponse.status} ${geminiResponse.statusText}`, errorText);
                                throw new Error(`AI OCR failed: ${geminiResponse.statusText}`);
                            }

                            const geminiResult = await geminiResponse.json();
                            console.log(`Gemini OCR finished for qid ${questionId} in ${ocrDuration}ms`);

                            // 提取識別文本 (注意路徑可能變化，需要根據實際響應調整)
                            let recognizedText = '';
                            try {
                                recognizedText = geminiResult.candidates[0].content.parts[0].text.trim();
                                // 清理可能的換行符等
                                recognizedText = recognizedText.replace(/[\r\n]+/g, '');
                            } catch (e) {
                                console.error(`Error parsing Gemini response for qid ${questionId}:`, e, JSON.stringify(geminiResult));
                                recognizedText = "[OCR 解析錯誤]";
                            }

                            // 3. 比對和評分
                            let isCorrect = false;
                            let score = 0;
                            if (correctAnswer === undefined) {
                                recognizedText += " [無法評分：未找到標準答案]";
                            } else if (recognizedText === correctAnswer) {
                                isCorrect = true;
                                score = 2; // 滿分 2 分
                            } else {
                                // 可以加入更細緻的錯誤標記，但目前是嚴格模式
                                score = 0;
                            }

                            return {
                                questionId: questionId,
                                success: true,
                                r2Key: key,
                                recognizedText: recognizedText,
                                isCorrect: isCorrect,
                                score: score,
                                correctAnswer: correctAnswer // 返回正確答案給前端用於顯示
                            };

                        } catch (err: any) {
                            console.error(`Processing failed for qid ${questionId}:`, err);
                            return { questionId: questionId, success: false, error: err.message };
                        }
                    } else { // 解碼失敗
                         return { questionId: questionId, success: false, error: 'Image data decoding failed.' };
                    }
                } else { // 空白答案
                    return {
                        questionId: questionId,
                        success: true,
                        r2Key: null,
                        recognizedText: "[空白]",
                        isCorrect: false,
                        score: 0, // 空白計 0 分
                        correctAnswer: correctAnswer ?? "[未找到標準答案]"
                    };
                }
            });

            // 等待所有答案處理完成
            const results = await Promise.all(processingPromises);

            // 計算總分
            const totalScore = results.reduce((sum, r) => sum + (r.success ? r.score || 0 : 0), 0);

            // TODO: 在這裡調用 Gemini 生成斥罵和解析 (基於 results)

            const responseData = {
                message: "評分完成。",
                totalScore: totalScore,
                results: results, // 返回每個題目的詳細處理結果
                // TODO: 添加 AI 生成的總體評價和解析
            };

            return new Response(JSON.stringify(responseData), { headers });
        }

        // --- 未匹配的路由 ---
        return new Response(JSON.stringify({ error: `API route /api/${apiPath} not found` }), { status: 404, headers });

    } catch (error: any) {
        console.error(`Error processing /api/${apiPath}:`, error);
        // 避免暴露 API Key 等敏感信息
        const errorMessage = (error.message.includes('configuration error') || error.message.includes('Invalid request'))
                             ? error.message
                             : 'Internal Server Error';
        return new Response(JSON.stringify({ error: errorMessage }), { status: 500, headers });
    }
};