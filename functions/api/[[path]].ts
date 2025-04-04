import type { PagesFunction } from '@cloudflare/workers-types';

// 使用靜態導入 JSON 文件，這是 Pages Functions 推薦的方式
// 確保相對路徑正確：從 functions/api/ 到根目錄下的 data/
import kaoshifanwei from '../../data/kaoshifanwei.json';
import zhenti from '../../data/zhenti.json';

interface Env {
    // 在這裡定義環境變量、綁定等 (如果需要)
    // 例如: GEMINI_API_KEY: string;
    //      IMAGES_BUCKET: R2Bucket;
}

// 輔助函數：從數組中隨機選取指定數量的元素
function getRandomItems<T>(arr: T[], num: number): T[] {
    if (!Array.isArray(arr)) {
        console.error("getRandomItems: input is not an array", arr);
        return [];
    }
    const shuffled = [...arr].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, num);
}

// Cloudflare Pages Function 的主處理函數
export const onRequest: PagesFunction<Env> = async (context) => {
    const { request, env, params } = context;
    const url = new URL(request.url);
    // params.path 是一個字符串數組，包含了 [[path]].ts 匹配到的路徑部分
    const pathSegments = params.path as string[];
    const apiPath = pathSegments.join('/'); // 組合路徑，例如 "hello" 或 "question"

    console.log(`[${new Date().toISOString()}] Request: ${request.method} /api/${apiPath}`);

    // 處理 CORS 預檢請求 (通常同源不需要，但加上更健壯)
    if (request.method === 'OPTIONS') {
        return new Response(null, {
            headers: {
                'Access-Control-Allow-Origin': '*', // 生產環境應更嚴格
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
            },
        });
    }

    const headers = {
         'Content-Type': 'application/json;charset=UTF-8',
         'Access-Control-Allow-Origin': '*' // 允許跨域（如果需要）
    };

    try {
        // --- API 路由 ---
        if (apiPath === 'hello' && request.method === 'GET') {
            const dataInfo = {
                message: '墨力全開後端 Function 已啟動！',
                status: 'OK',
                kaoshiFanweiLoaded: kaoshifanwei ? `Loaded ${kaoshifanwei.文言文?.length || 0} 文言文, ${kaoshifanwei.诗词曲?.length || 0} 詩詞曲` : 'Failed',
                zhentiLoaded: zhenti ? `Loaded ${zhenti?.length || 0} 真題 items` : 'Failed',
                timestamp: new Date().toISOString()
            };
            // 檢查數據是否真的加載成功
            if (!kaoshifanwei || !zhenti || zhenti.length === 0) {
                console.warn("Warning: Data files might not be loaded correctly. Check imports and file paths.");
                dataInfo.status = "Warning: Data load issue detected.";
            }
            return new Response(JSON.stringify(dataInfo), { headers });
        }

        if (apiPath === 'question' && request.method === 'GET') {
            if (!kaoshifanwei || !zhenti || zhenti.length === 0) {
                console.error("Error: Prerequisite data (zhenti, kaoshifanwei) not available.");
                throw new Error("後端數據準備失敗，無法生成題目。");
            }

            // --- 基礎命題邏輯 (從真題中隨機抽取) ---
            // TODO: 實現更高級的、基於模式分析和語料庫生成的命題邏輯
            const numberOfQuestions = 4;
            const randomZhentiItems = getRandomItems(zhenti, numberOfQuestions);

            if (randomZhentiItems.length < numberOfQuestions) {
                console.warn(`Warning: Could only fetch ${randomZhentiItems.length} items from zhenti, requested ${numberOfQuestions}.`);
                 // 可以選擇填充或返回部分題目
            }

            const questions = randomZhentiItems.map((item: any, index: number) => {
                 // 嘗試從答案反查篇名 (簡易版)
                 let source = "未知來源";
                 if (item.reference_answer) {
                     const foundSource = kaoshifanwei.文言文.find(p => p.content.includes(item.reference_answer))?.title ||
                                       kaoshifanwei.诗词曲.find(p => p.content.includes(item.reference_answer))?.title;
                     if (foundSource) source = foundSource;
                 }

                return {
                    id: index + 1, // 臨時 ID
                    topic: item.topic || "在横线处填写作品原句。(共2分)", // 提供默認值
                    question: item.question || "題目描述缺失", // 提供默認值
                    reference_answer: item.reference_answer || "答案缺失", // 記錄正確答案 (實際不應返回給前端)
                    source: source // 添加來源
                };
            });

            // 重要：實際應用中不應將 reference_answer 直接返回給前端
            const questionsForFrontend = questions.map(({ reference_answer, ...rest }) => rest);

            return new Response(JSON.stringify(questionsForFrontend), { headers });
             // --- 基礎命題邏輯結束 ---
        }

        if (apiPath === 'submit' && request.method === 'POST') {
            // TODO: 實現手寫識別、評分邏輯
            console.log("Received POST on /api/submit. Body parsing and processing needed.");
            // const formData = await request.formData(); // 如果前端用 FormData 提交圖片
            // const jsonData = await request.json(); // 如果前端用 JSON 提交數據 (如 Base64)
            // ... 處理數據 ...
            await new Promise(resolve => setTimeout(resolve, 500)); // 模擬處理
            return new Response(JSON.stringify({ message: "答案提交成功 (處理邏輯待實現)" }), { headers });
        }

        // --- 未匹配的路由 ---
        return new Response(JSON.stringify({ error: `API route /api/${apiPath} not found or method ${request.method} not allowed.` }), {
            status: 404,
            headers
        });

    } catch (error: any) {
        console.error(`Error processing /api/${apiPath}:`, error);
        return new Response(JSON.stringify({ error: error.message || 'Internal Server Error' }), {
            status: 500,
            headers
        });
    }
};