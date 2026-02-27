import type { PagesFunction, R2Bucket, KVNamespace } from '@cloudflare/workers-types';

// --- NEW DATA LOADING --- (Requirement 1 & 2)
// Load poem data from poems.json
import poemsDataRaw from './_internal/poems.json';

// --- Constants ---
const GAOKAO_SET_TTL_SECONDS = 60 * 60 * 6; // 6 hours
// ** Using specified models **
const GEMINI_VISION_MODEL = "gemini-flash-latest";
const GEMINI_TEXT_MODEL = "gemini-flash-latest";
const GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
const MAX_QUESTIONS_PER_GAOKAO_SET = 4; // Target number of questions for "挑戰高考"
const MAX_FEEDBACK_TOKENS = 3000;
const API_RETRY_COUNT = 2;
const API_RETRY_DELAY_MS = 1500;
const SCORE_PER_QUESTION = 2;
const REFERENCE_QUESTION_COUNT_FOR_POINTS = 4;
const MAX_POINTS_BASE = 80;
const LEADERBOARD_TOP_LIMIT = 50;
const LEADERBOARD_SCAN_BATCH_SIZE = 1000;
const LEADERBOARD_CACHE_TTL_SECONDS = 60;
const LEADERBOARD_VERSION_KEY = 'moxie:leaderboard:version';
const STATS_PERIOD_TTL_SECONDS = 60 * 60 * 24 * 120; // 120 days
const SESSION_TTL_SECONDS = 60 * 60 * 24 * 14; // 14 days
const SUBMIT_COOLDOWN_SECONDS = 45;
const SUBMIT_LOCK_TTL_SECONDS = 25;
const GAOKAO_SET_LOCK_TTL_SECONDS = 300;
const CHAPTER_DAILY_COUNTER_TTL_SECONDS = 60 * 60 * 24 * 2;
const CHAPTER_WEEKLY_COUNTER_TTL_SECONDS = 60 * 60 * 24 * 8;
const MAX_UPLOAD_IMAGE_BYTES = 5 * 1024 * 1024;
const CHAPTER_DAILY_SUBMIT_LIMIT = 2;
const CHAPTER_WEEKLY_SUBMIT_LIMIT = 8;
const IMAGE_FINGERPRINT_PENDING_TTL_SECONDS = 60 * 10;
const IMAGE_FINGERPRINT_TTL_SECONDS = 60 * 60 * 24;
const USERNAME_MIN_LENGTH = 2;
const USERNAME_MAX_LENGTH = 20;
const USERNAME_ALLOWED_REGEX = /^[\p{L}\p{N}_-]+$/u;
const PASSWORD_MIN_LENGTH = 6;
const PASSWORD_MAX_LENGTH = 64;
const PASSWORD_HASH_ITERATIONS = 100000;
const CHANGE_PASSWORD_WINDOW_SECONDS = 60 * 60;
const CHANGE_PASSWORD_MAX_ATTEMPTS = 5;

const ALLOWED_ORIGIN_PATTERNS = [
    /^https:\/\/([a-z0-9-]+\.)*bdfz\.net$/i,
    /^https:\/\/([a-z0-9-]+\.)*bdfzer\.com$/i,
    /^https:\/\/([a-z0-9-]+\.)*rdfzer\.com$/i,
    /^https:\/\/[a-z0-9-]+\.pages\.dev$/i,
    /^http:\/\/localhost(?::\d+)?$/i,
    /^http:\/\/127\.0\.0\.1(?::\d+)?$/i,
];

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

interface ChapterSummary {
    order: number;
    title: string;
}

interface PreparedPoemData {
    entries: ProcessedPoemEntry[];
    chapterList: ChapterSummary[];
    chapterMap: Map<number, ProcessedPoemEntry>;
    allQuestions: PoemQuestion[];
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
    ownerUserId: string;
    usedAt?: number;
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

type LeaderboardScope = 'total' | 'weekly' | 'daily';

interface UserProfile {
    userId: string;
    username: string;
    createdAt: number;
    updatedAt: number;
}

interface UserAuthRecord {
    userId: string;
    username: string;
    passwordSalt: string;
    passwordHash: string;
    activeSessionToken?: string;
    createdAt: number;
    updatedAt: number;
}

interface SessionRecord {
    token: string;
    userId: string;
    username: string;
    createdAt: number;
    expiresAt: number;
}

interface UserStats {
    userId: string;
    username: string;
    points: number;
    attempts: number;
    totalScore: number;
    updatedAt: number;
}

interface TierInfo {
    tierLevel: number;
    tierName: string;
    modeName: string;
    nextTierName: string | null;
    progressPercent: number;
}

interface LeaderboardEntry extends UserStats {
    avgScore: number;
    tier: TierInfo;
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

function normalizePathValue(pathValue: string | string[] | undefined): string {
    if (Array.isArray(pathValue)) {
        return pathValue
            .map(part => part.trim())
            .filter(Boolean)
            .join('/');
    }

    if (typeof pathValue === 'string') {
        return pathValue
            .split('/')
            .map(part => part.trim())
            .filter(Boolean)
            .join('/');
    }

    return '';
}

function resolveApiPath(params: Record<string, string | string[]>, pathname: string): string {
    const fromParam = normalizePathValue(params.path);
    if (fromParam) {
        return decodeURIComponent(fromParam);
    }

    const segments = pathname
        .split('/')
        .map(part => part.trim())
        .filter(Boolean);

    if (segments[0] !== 'api') {
        return '';
    }

    return decodeURIComponent(segments.slice(1).join('/'));
}

function getMissingBindings(env: Env, required: readonly (keyof Env)[]): (keyof Env)[] {
    return required.filter(binding => !env[binding]);
}

function createMissingBindingsResponse(missing: (keyof Env)[], headers: Record<string, string>): Response {
    console.error(`Server configuration error: Missing bindings/secrets -> ${missing.join(', ')}`);
    return new Response(JSON.stringify({ error: "Server configuration error. Please contact administrator." }), {
        status: 500,
        headers,
    });
}

function normalizeUsername(input: string): string {
    return input.trim().toLowerCase();
}

function validateUsername(input: string): { normalized: string; display: string } {
    const display = input.trim();
    if (display.length < USERNAME_MIN_LENGTH || display.length > USERNAME_MAX_LENGTH) {
        throw new Error(`用戶名長度需在 ${USERNAME_MIN_LENGTH}-${USERNAME_MAX_LENGTH} 字之間。`);
    }
    if (!USERNAME_ALLOWED_REGEX.test(display)) {
        throw new Error("用戶名僅支持中文、字母、數字、下劃線與連字符。");
    }
    return { normalized: normalizeUsername(display), display };
}

function validatePassword(input: string): string {
    if (typeof input !== 'string') {
        throw new Error('請輸入密碼。');
    }
    const password = input.trim();
    if (password.length < PASSWORD_MIN_LENGTH || password.length > PASSWORD_MAX_LENGTH) {
        throw new Error(`密碼長度需在 ${PASSWORD_MIN_LENGTH}-${PASSWORD_MAX_LENGTH} 字之間。`);
    }
    return password;
}

function createHttpError(status: number, message: string): Error & { status: number } {
    const error = new Error(message) as Error & { status: number };
    error.status = status;
    return error;
}

function isAllowedOrigin(origin: string | null, request?: Request): boolean {
    if (!origin) {
        // Same-origin browser requests may omit Origin; allow only in trusted fetch contexts.
        const secFetchSite = request?.headers.get('Sec-Fetch-Site');
        if (!secFetchSite) {
            return true;
        }
        return secFetchSite === 'same-origin' || secFetchSite === 'same-site' || secFetchSite === 'none';
    }
    return ALLOWED_ORIGIN_PATTERNS.some(pattern => pattern.test(origin));
}

function buildCorsHeaders(origin: string | null): Record<string, string> {
    const headers: Record<string, string> = {
        'Content-Type': 'application/json;charset=UTF-8',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Vary': 'Origin',
    };
    if (origin && isAllowedOrigin(origin)) {
        headers['Access-Control-Allow-Origin'] = origin;
    }
    return headers;
}

function getShanghaiDateTimeParts(nowMs: number): { year: number; month: number; day: number } {
    const shifted = new Date(nowMs + 8 * 60 * 60 * 1000);
    return {
        year: shifted.getUTCFullYear(),
        month: shifted.getUTCMonth() + 1,
        day: shifted.getUTCDate(),
    };
}

function getDailyKey(nowMs = Date.now()): string {
    const { year, month, day } = getShanghaiDateTimeParts(nowMs);
    return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

function getWeeklyKey(nowMs = Date.now()): string {
    const { year, month, day } = getShanghaiDateTimeParts(nowMs);
    const date = new Date(Date.UTC(year, month - 1, day));
    // ISO week: shift to Thursday of current week
    const weekday = date.getUTCDay() || 7;
    date.setUTCDate(date.getUTCDate() + 4 - weekday);
    const weekYear = date.getUTCFullYear();
    const yearStart = new Date(Date.UTC(weekYear, 0, 1));
    const weekNo = Math.ceil((((date.getTime() - yearStart.getTime()) / 86400000) + 1) / 7);
    return `${weekYear}-W${String(weekNo).padStart(2, '0')}`;
}

function getProfileKey(userId: string): string {
    return `moxie:user:profile:${userId}`;
}

function getAuthKey(userId: string): string {
    return `moxie:user:auth:${userId}`;
}

function getSessionKey(token: string): string {
    return `moxie:session:${token}`;
}

function getChangePasswordRateKey(userId: string): string {
    return `moxie:auth:change-password-rate:${userId}`;
}

function getStatsKey(scope: LeaderboardScope, period: string, userId: string): string {
    if (scope === 'total') {
        return `moxie:leaderboard:total:user:${userId}`;
    }
    return `moxie:leaderboard:${scope}:${period}:user:${userId}`;
}

function getStatsPrefix(scope: LeaderboardScope, period: string): string {
    if (scope === 'total') {
        return `moxie:leaderboard:total:user:`;
    }
    return `moxie:leaderboard:${scope}:${period}:user:`;
}

function getGaokaoSetKey(setId: string): string {
    return `moxie:gaokao:set:${setId}`;
}

function getGaokaoSetLockKey(setId: string): string {
    return `moxie:gaokao:set-lock:${setId}`;
}

function getSubmitLockKey(userId: string): string {
    return `moxie:submit:lock:${userId}`;
}

function getSubmitCooldownKey(userId: string): string {
    return `moxie:submit:cooldown:${userId}`;
}

function getChapterAttemptKey(scope: 'daily' | 'weekly', period: string, userId: string, chapterOrder: number): string {
    return `moxie:chapter-attempt:${scope}:${period}:user:${userId}:chapter:${chapterOrder}`;
}

function getImageFingerprintKey(userId: string, fingerprint: string): string {
    return `moxie:image-fingerprint:user:${userId}:${fingerprint}`;
}

function getLeaderboardScopeCacheKey(scope: LeaderboardScope, period: string, limit: number, version: string): string {
    return `moxie:leaderboard:cache:${scope}:${period}:limit:${limit}:version:${version}`;
}

async function getJsonKV<T>(kv: KVNamespace, key: string): Promise<T | null> {
    return kv.get<T>(key, 'json');
}

async function putJsonKV(kv: KVNamespace, key: string, value: unknown, expirationTtl?: number): Promise<void> {
    const options = expirationTtl ? { expirationTtl } : undefined;
    await kv.put(key, JSON.stringify(value), options);
}

function getScoreTarget(questionCount: number): number {
    const safeQuestionCount = Number.isFinite(questionCount) ? questionCount : 1;
    return Math.max(1, safeQuestionCount) * SCORE_PER_QUESTION;
}

function parseStoredCounter(rawValue: string | null, fallbackOnInvalid = 0): number {
    if (rawValue === null) {
        return 0;
    }
    const parsed = parseInt(rawValue, 10);
    if (!Number.isFinite(parsed) || parsed < 0) {
        return fallbackOnInvalid;
    }
    return parsed;
}

function calculatePointsAwarded(totalScore: number, scoreTarget: number, questionCount: number): number {
    const safeScoreTarget = Math.max(scoreTarget, 1);
    const accuracy = Math.max(0, Math.min(1, totalScore / safeScoreTarget));
    const difficultyFactor = Math.max(
        0.8,
        Math.min(1.5, Math.sqrt(Math.max(questionCount, 1) / REFERENCE_QUESTION_COUNT_FOR_POINTS))
    );
    const basePoints = Math.round(MAX_POINTS_BASE * accuracy * difficultyFactor);
    const participationBonus = 5;
    const fullScoreBonus = accuracy >= 1 ? 15 : 0;
    return basePoints + participationBonus + fullScoreBonus;
}

const IDV_TIERS: { minPoints: number; tierLevel: number; tierName: string; modeName: string; }[] = [
    { minPoints: 0, tierLevel: 1, tierName: '工蜂', modeName: '常規排位' },
    { minPoints: 200, tierLevel: 2, tierName: '獵犬', modeName: '常規排位' },
    { minPoints: 600, tierLevel: 3, tierName: '馴鹿', modeName: '常規排位' },
    { minPoints: 1200, tierLevel: 4, tierName: '猛獁', modeName: '常規排位' },
    { minPoints: 2000, tierLevel: 5, tierName: '獅鷲', modeName: '常規排位' },
    { minPoints: 3200, tierLevel: 6, tierName: '獨角獸', modeName: '常規排位' },
    { minPoints: 4800, tierLevel: 7, tierName: '殿堂勇士', modeName: '殿堂模式' },
    { minPoints: 7000, tierLevel: 8, tierName: '巔峰泰坦', modeName: '巔峰模式' },
];

function getTierInfo(points: number): TierInfo {
    let currentIndex = 0;
    for (let i = 0; i < IDV_TIERS.length; i++) {
        if (points >= IDV_TIERS[i].minPoints) {
            currentIndex = i;
        } else {
            break;
        }
    }

    const current = IDV_TIERS[currentIndex];
    const next = IDV_TIERS[currentIndex + 1] ?? null;
    let progressPercent = 100;
    if (next) {
        const span = next.minPoints - current.minPoints;
        progressPercent = Math.max(0, Math.min(100, Math.round(((points - current.minPoints) / span) * 100)));
    }

    return {
        tierLevel: current.tierLevel,
        tierName: current.tierName,
        modeName: current.modeName,
        nextTierName: next?.tierName ?? null,
        progressPercent,
    };
}

function toLeaderboardEntry(stats: UserStats): LeaderboardEntry {
    return {
        ...stats,
        avgScore: stats.attempts > 0 ? Math.round((stats.totalScore / stats.attempts) * 100) / 100 : 0,
        tier: getTierInfo(stats.points),
    };
}

function sortLeaderboard(entries: LeaderboardEntry[]): LeaderboardEntry[] {
    return entries.sort((a, b) => {
        if (b.points !== a.points) return b.points - a.points;
        if (b.attempts !== a.attempts) return b.attempts - a.attempts;
        return a.updatedAt - b.updatedAt;
    });
}

async function updateScopedStats(
    kv: KVNamespace,
    scope: LeaderboardScope,
    period: string,
    userId: string,
    username: string,
    pointsAwarded: number,
    totalScore: number
): Promise<UserStats> {
    const statsKey = getStatsKey(scope, period, userId);
    const now = Date.now();
    const current = await getJsonKV<UserStats>(kv, statsKey);
    const nextStats: UserStats = {
        userId,
        username,
        points: (current?.points ?? 0) + pointsAwarded,
        attempts: (current?.attempts ?? 0) + 1,
        totalScore: Math.round(((current?.totalScore ?? 0) + totalScore) * 10) / 10,
        updatedAt: now,
    };

    const ttl = scope === 'total' ? undefined : STATS_PERIOD_TTL_SECONDS;
    await putJsonKV(kv, statsKey, nextStats, ttl);
    return nextStats;
}

async function ensureUserProfile(kv: KVNamespace, userId: string, username: string): Promise<UserProfile> {
    const key = getProfileKey(userId);
    const current = await getJsonKV<UserProfile>(kv, key);
    const now = Date.now();
    const profile: UserProfile = {
        userId,
        username,
        createdAt: current?.createdAt ?? now,
        updatedAt: now,
    };
    await putJsonKV(kv, key, profile);
    return profile;
}

function toBase64Url(bytes: Uint8Array): string {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function generateSecureToken(size = 32): string {
    const bytes = new Uint8Array(size);
    crypto.getRandomValues(bytes);
    return toBase64Url(bytes);
}

async function hashSha256Base64Url(raw: ArrayBuffer | string): Promise<string> {
    const bytes = typeof raw === 'string' ? new TextEncoder().encode(raw) : new Uint8Array(raw);
    const digest = await crypto.subtle.digest('SHA-256', bytes);
    return toBase64Url(new Uint8Array(digest));
}

async function hashPassword(password: string, salt: string): Promise<string> {
    const encoder = new TextEncoder();
    const keyMaterial = await crypto.subtle.importKey(
        'raw',
        encoder.encode(password),
        'PBKDF2',
        false,
        ['deriveBits']
    );
    const hashBuffer = await crypto.subtle.deriveBits(
        {
            name: 'PBKDF2',
            hash: 'SHA-256',
            salt: encoder.encode(salt),
            iterations: PASSWORD_HASH_ITERATIONS,
        },
        keyMaterial,
        256
    );
    return toBase64Url(new Uint8Array(hashBuffer));
}

function timingSafeEqual(a: string, b: string): boolean {
    const maxLength = Math.max(a.length, b.length);
    let mismatch = 0;
    for (let i = 0; i < maxLength; i++) {
        const charA = i < a.length ? a.charCodeAt(i) : 0;
        const charB = i < b.length ? b.charCodeAt(i) : 0;
        mismatch |= charA ^ charB;
    }
    return mismatch === 0 && a.length === b.length;
}

async function authenticateAndIssueSession(
    kv: KVNamespace,
    userId: string,
    username: string,
    password: string
): Promise<{ session: SessionRecord; isNewUser: boolean; authRecord: UserAuthRecord; }> {
    const authKey = getAuthKey(userId);
    const now = Date.now();
    const existingAuth = await getJsonKV<UserAuthRecord>(kv, authKey);
    let isNewUser = false;
    let authRecord: UserAuthRecord;

    if (!existingAuth) {
        const passwordSalt = generateSecureToken(16);
        const passwordHash = await hashPassword(password, passwordSalt);
        authRecord = {
            userId,
            username,
            passwordSalt,
            passwordHash,
            createdAt: now,
            updatedAt: now,
        };
        isNewUser = true;
    } else {
        const passwordHash = await hashPassword(password, existingAuth.passwordSalt);
        if (!timingSafeEqual(passwordHash, existingAuth.passwordHash)) {
            throw createHttpError(401, '用戶名或密碼錯誤。');
        }
        if (existingAuth.username !== username) {
            existingAuth.username = username;
        }
        authRecord = {
            ...existingAuth,
            updatedAt: now,
        };
    }

    if (authRecord.activeSessionToken) {
        await kv.delete(getSessionKey(authRecord.activeSessionToken)).catch(() => undefined);
    }

    await ensureUserProfile(kv, userId, username);
    const token = generateSecureToken(32);
    const session: SessionRecord = {
        token,
        userId,
        username,
        createdAt: now,
        expiresAt: now + (SESSION_TTL_SECONDS * 1000),
    };
    await putJsonKV(kv, getSessionKey(token), session, SESSION_TTL_SECONDS);
    authRecord.activeSessionToken = token;
    authRecord.updatedAt = now;
    await putJsonKV(kv, authKey, authRecord);
    return { session, isNewUser, authRecord };
}

function extractBearerToken(request: Request): string | null {
    const authHeader = request.headers.get('Authorization');
    if (!authHeader) return null;
    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    if (!match) return null;
    const token = match[1].trim();
    return token || null;
}

async function getSessionFromRequest(request: Request, kv: KVNamespace, required = true): Promise<SessionRecord | null> {
    const token = extractBearerToken(request);
    if (!token) {
        if (!required) return null;
        throw createHttpError(401, '請先登錄。');
    }
    const session = await getJsonKV<SessionRecord>(kv, getSessionKey(token));
    if (!session || session.expiresAt <= Date.now() || session.userId.length === 0) {
        await kv.delete(getSessionKey(token)).catch(() => undefined);
        throw createHttpError(401, '登錄已過期，請重新登錄。');
    }
    const authRecord = await getJsonKV<UserAuthRecord>(kv, getAuthKey(session.userId));
    if (!authRecord || authRecord.activeSessionToken !== token) {
        await kv.delete(getSessionKey(token)).catch(() => undefined);
        throw createHttpError(401, '登錄已失效，請重新登錄。');
    }
    return session;
}

async function getUserStatsSummary(kv: KVNamespace, userId: string, username: string): Promise<{
    total: LeaderboardEntry;
    weekly: LeaderboardEntry;
    daily: LeaderboardEntry;
    dailyKey: string;
    weeklyKey: string;
}> {
    const now = Date.now();
    const dailyKey = getDailyKey(now);
    const weeklyKey = getWeeklyKey(now);

    const totalStats = await getJsonKV<UserStats>(kv, getStatsKey('total', 'all', userId)) ?? {
        userId,
        username,
        points: 0,
        attempts: 0,
        totalScore: 0,
        updatedAt: now,
    };
    const weeklyStats = await getJsonKV<UserStats>(kv, getStatsKey('weekly', weeklyKey, userId)) ?? {
        userId,
        username,
        points: 0,
        attempts: 0,
        totalScore: 0,
        updatedAt: now,
    };
    const dailyStats = await getJsonKV<UserStats>(kv, getStatsKey('daily', dailyKey, userId)) ?? {
        userId,
        username,
        points: 0,
        attempts: 0,
        totalScore: 0,
        updatedAt: now,
    };

    return {
        total: toLeaderboardEntry(totalStats),
        weekly: toLeaderboardEntry(weeklyStats),
        daily: toLeaderboardEntry(dailyStats),
        dailyKey,
        weeklyKey,
    };
}

async function getLeaderboardVersion(kv: KVNamespace): Promise<string> {
    const raw = await kv.get(LEADERBOARD_VERSION_KEY);
    if (!raw) {
        return '0';
    }
    const normalized = raw.trim();
    return normalized || '0';
}

async function bumpLeaderboardVersion(kv: KVNamespace): Promise<string> {
    // Use unique version token to avoid read-then-write race conditions.
    const next = `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;
    await kv.put(LEADERBOARD_VERSION_KEY, String(next));
    return next;
}

async function readLeaderboardScope(
    kv: KVNamespace,
    scope: LeaderboardScope,
    period: string,
    limit: number,
    version: string
): Promise<LeaderboardEntry[]> {
    const safeLimit = Math.max(1, Math.min(limit, LEADERBOARD_TOP_LIMIT));
    const cacheKey = getLeaderboardScopeCacheKey(scope, period, safeLimit, version);
    let cached: LeaderboardEntry[] | null = null;
    try {
        cached = await getJsonKV<LeaderboardEntry[]>(kv, cacheKey);
    } catch (cacheError: any) {
        console.warn(`Failed to parse leaderboard cache key ${cacheKey}, rebuilding cache:`, cacheError?.message || cacheError);
        await kv.delete(cacheKey).catch(() => undefined);
    }
    if (cached) {
        return cached.slice(0, safeLimit);
    }

    const prefix = getStatsPrefix(scope, period);
    let cursor: string | undefined = undefined;
    const stats: UserStats[] = [];

    while (true) {
        const listResult = await kv.list({ prefix, cursor, limit: LEADERBOARD_SCAN_BATCH_SIZE });
        if (listResult.keys.length > 0) {
            const batchStats = await Promise.all(
                listResult.keys.map(async (key) => {
                    try {
                        return await getJsonKV<UserStats>(kv, key.name);
                    } catch (entryError: any) {
                        // Skip malformed legacy/bad records rather than failing the whole leaderboard.
                        console.warn(`Skipping malformed leaderboard record key ${key.name}:`, entryError?.message || entryError);
                        return null;
                    }
                })
            );
            for (const item of batchStats) {
                if (item) stats.push(item);
            }
        }
        if (listResult.list_complete) break;
        cursor = listResult.cursor;
    }

    const topList = sortLeaderboard(stats.map(toLeaderboardEntry)).slice(0, safeLimit);
    await putJsonKV(kv, cacheKey, topList, LEADERBOARD_CACHE_TTL_SECONDS);
    return topList;
}

async function getLeaderboardBundle(kv: KVNamespace, limit: number): Promise<{
    period: { dailyKey: string; weeklyKey: string; };
    total: LeaderboardEntry[];
    weekly: LeaderboardEntry[];
    daily: LeaderboardEntry[];
}> {
    const now = Date.now();
    const dailyKey = getDailyKey(now);
    const weeklyKey = getWeeklyKey(now);
    const safeLimit = Math.max(1, Math.min(limit, LEADERBOARD_TOP_LIMIT));
    const version = await getLeaderboardVersion(kv);

    const [total, weekly, daily] = await Promise.all([
        readLeaderboardScope(kv, 'total', 'all', safeLimit, version),
        readLeaderboardScope(kv, 'weekly', weeklyKey, safeLimit, version),
        readLeaderboardScope(kv, 'daily', dailyKey, safeLimit, version),
    ]);

    return {
        period: { dailyKey, weeklyKey },
        total,
        weekly,
        daily,
    };
}

// --- Gemini API Call Function (Unchanged) ---
async function callGeminiAPI(apiKey: string, model: string, contents: GeminiContent[], generationConfig?: { maxOutputTokens?: number; temperature?: number; }): Promise<GeminiApiResponse> {
    // ... (Implementation unchanged from the provided snippet, including retry logic)
    const url = `${GEMINI_API_BASE_URL}${model}:generateContent`;
    let lastError: any = null;

    for (let attempt = 0; attempt <= API_RETRY_COUNT; attempt++) {
        console.log(`Calling Gemini API (Model: ${model}, Attempt: ${attempt + 1}/${API_RETRY_COUNT + 1})`);
        let response: Response | null = null; // Declare response outside try

        try {
            response = await fetch(url, { // Assign to outer response
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-goog-api-key': apiKey,
                },
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

// Global cache to avoid repeated parsing and flattening on each request.
let preparedPoemData: PreparedPoemData | null = null;

function getPreparedPoemData(): PreparedPoemData {
    if (preparedPoemData) {
        return preparedPoemData;
    }

    if (!isValidPoemsData(poemsDataRaw)) {
        throw new Error("poems.json data failed validation.");
    }

    const entries = transformRawPoemsData(poemsDataRaw);
    const chapterList: ChapterSummary[] = entries
        .map(entry => ({ order: entry.order, title: entry.title }))
        .sort((a, b) => a.order - b.order);
    const chapterMap = new Map<number, ProcessedPoemEntry>();
    const allQuestions: PoemQuestion[] = [];

    for (const entry of entries) {
        chapterMap.set(entry.order, entry);
        for (const q of entry.questions) {
            allQuestions.push({
                question: q.question,
                answer: q.answer,
                year: q.year,
                sourceTitle: entry.title,
                sourceAuthor: entry.author,
                sourceDynasty: entry.dynasty,
                sourceCategory: entry.category,
                sourceOrder: entry.order,
            });
        }
    }

    preparedPoemData = { entries, chapterList, chapterMap, allQuestions };
    console.log(`Successfully validated and prepared poems.json data. Entries: ${entries.length}, Questions: ${allQuestions.length}`);
    return preparedPoemData;
}

// --- Main Request Handler ---
export const onRequest: PagesFunction<Env> = async (context) => {
    console.log("onRequest: API request received");
    const { request, env, params } = context;
    const url = new URL(request.url);
    const apiPath = resolveApiPath(params as Record<string, string | string[]>, url.pathname);
    // Extract query parameters for chapter selection
    const chapterOrderParam = url.searchParams.get('order');
    const origin = request.headers.get('Origin');
    const baseHeaders = buildCorsHeaders(origin);
    const originAllowed = isAllowedOrigin(origin, request);

    if (request.method === 'OPTIONS') {
        if (!originAllowed) {
            return new Response(JSON.stringify({ error: 'Origin not allowed.' }), { status: 403, headers: baseHeaders });
        }
        return new Response(null, { status: 204, headers: baseHeaders });
    }

    if (!originAllowed) {
        return new Response(JSON.stringify({ error: 'Origin not allowed.' }), { status: 403, headers: baseHeaders });
    }

    console.log(`[${new Date().toISOString()}] Request: ${request.method} /api/${apiPath}${url.search}`);

    try {
        // --- API Routing ---

        if (apiPath === 'hello' && request.method === 'GET') {
            const dataInfo = {
                message: "Backend is running.",
                status: "OK",
                timestamp: new Date().toISOString(),
            };
            return new Response(JSON.stringify(dataInfo), { headers: baseHeaders });
        }

        if (apiPath === 'login' && request.method === 'POST') {
            const missingBindings = getMissingBindings(env, ['SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }

            let requestBody: any = null;
            try {
                requestBody = await request.json();
            } catch (parseError) {
                return new Response(JSON.stringify({ error: '請求格式錯誤：需提交 JSON 格式。' }), { status: 400, headers: baseHeaders });
            }

            const rawUsername = typeof requestBody?.username === 'string' ? requestBody.username : '';
            const rawPassword = typeof requestBody?.password === 'string' ? requestBody.password : '';
            if (!rawUsername) {
                return new Response(JSON.stringify({ error: '請輸入用戶名。' }), { status: 400, headers: baseHeaders });
            }

            let normalized = '';
            let display = '';
            let password = '';
            try {
                const validated = validateUsername(rawUsername);
                normalized = validated.normalized;
                display = validated.display;
                password = validatePassword(rawPassword);
            } catch (validationError: any) {
                return new Response(JSON.stringify({ error: validationError.message || '登錄信息格式不正確。' }), { status: 400, headers: baseHeaders });
            }

            const { session, isNewUser } = await authenticateAndIssueSession(env.SESSION_KV, normalized, display, password);
            const stats = await getUserStatsSummary(env.SESSION_KV, session.userId, session.username);

            return new Response(JSON.stringify({
                user: {
                    userId: session.userId,
                    username: session.username,
                },
                stats,
                token: session.token,
                expiresAt: session.expiresAt,
                isNewUser,
            }), { headers: baseHeaders });
        }

        if (apiPath === 'me' && request.method === 'GET') {
            const missingBindings = getMissingBindings(env, ['SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }

            const session = await getSessionFromRequest(request, env.SESSION_KV, true);
            const stats = await getUserStatsSummary(env.SESSION_KV, session.userId, session.username);
            return new Response(JSON.stringify({
                user: {
                    userId: session.userId,
                    username: session.username,
                },
                stats,
                expiresAt: session.expiresAt,
            }), { headers: baseHeaders });
        }

        if (apiPath === 'logout' && request.method === 'POST') {
            const missingBindings = getMissingBindings(env, ['SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }

            const token = extractBearerToken(request);
            if (token) {
                const session = await getJsonKV<SessionRecord>(env.SESSION_KV, getSessionKey(token));
                if (session) {
                    const authRecord = await getJsonKV<UserAuthRecord>(env.SESSION_KV, getAuthKey(session.userId));
                    if (authRecord?.activeSessionToken === token) {
                        delete authRecord.activeSessionToken;
                        authRecord.updatedAt = Date.now();
                        await putJsonKV(env.SESSION_KV, getAuthKey(session.userId), authRecord);
                    }
                }
                await env.SESSION_KV.delete(getSessionKey(token));
            }
            return new Response(JSON.stringify({ success: true }), { headers: baseHeaders });
        }

        if (apiPath === 'change_password' && request.method === 'POST') {
            const missingBindings = getMissingBindings(env, ['SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }
            const session = await getSessionFromRequest(request, env.SESSION_KV, true);

            let requestBody: any = null;
            try {
                requestBody = await request.json();
            } catch {
                return new Response(JSON.stringify({ error: '請求格式錯誤：需提交 JSON 格式。' }), { status: 400, headers: baseHeaders });
            }

            const oldPasswordRaw = typeof requestBody?.oldPassword === 'string' ? requestBody.oldPassword : '';
            const newPasswordRaw = typeof requestBody?.newPassword === 'string' ? requestBody.newPassword : '';
            let oldPassword = '';
            let newPassword = '';
            try {
                oldPassword = validatePassword(oldPasswordRaw);
                newPassword = validatePassword(newPasswordRaw);
            } catch (validationError: any) {
                return new Response(JSON.stringify({ error: validationError.message || '密碼格式不正確。' }), { status: 400, headers: baseHeaders });
            }
            if (oldPassword === newPassword) {
                return new Response(JSON.stringify({ error: '新密碼不可與舊密碼相同。' }), { status: 400, headers: baseHeaders });
            }

            const authKey = getAuthKey(session.userId);
            const authRecord = await getJsonKV<UserAuthRecord>(env.SESSION_KV, authKey);
            if (!authRecord) {
                return new Response(JSON.stringify({ error: '賬號信息缺失，請重新登錄。' }), { status: 401, headers: baseHeaders });
            }

            const changePasswordRateKey = getChangePasswordRateKey(session.userId);
            const rawRateCount = await env.SESSION_KV.get(changePasswordRateKey);
            const rateCount = rawRateCount ? parseInt(rawRateCount, 10) : 0;
            const failedAttempts = Number.isFinite(rateCount) && rateCount > 0 ? rateCount : 0;
            if (failedAttempts >= CHANGE_PASSWORD_MAX_ATTEMPTS) {
                return new Response(
                    JSON.stringify({ error: `密碼驗證失敗次數過多，請於 ${Math.round(CHANGE_PASSWORD_WINDOW_SECONDS / 60)} 分鐘後再試。` }),
                    { status: 429, headers: baseHeaders }
                );
            }

            const oldHash = await hashPassword(oldPassword, authRecord.passwordSalt);
            if (!timingSafeEqual(oldHash, authRecord.passwordHash)) {
                await env.SESSION_KV.put(
                    changePasswordRateKey,
                    String(failedAttempts + 1),
                    { expirationTtl: CHANGE_PASSWORD_WINDOW_SECONDS }
                );
                return new Response(JSON.stringify({ error: '舊密碼不正確。' }), { status: 401, headers: baseHeaders });
            }

            const newSalt = generateSecureToken(16);
            const newHash = await hashPassword(newPassword, newSalt);
            const now = Date.now();
            const newToken = generateSecureToken(32);
            const newSession: SessionRecord = {
                token: newToken,
                userId: session.userId,
                username: session.username,
                createdAt: now,
                expiresAt: now + (SESSION_TTL_SECONDS * 1000),
            };
            await putJsonKV(env.SESSION_KV, getSessionKey(newToken), newSession, SESSION_TTL_SECONDS);
            await env.SESSION_KV.delete(getSessionKey(session.token)).catch(() => undefined);

            const updatedAuth: UserAuthRecord = {
                ...authRecord,
                passwordSalt: newSalt,
                passwordHash: newHash,
                updatedAt: now,
                activeSessionToken: newToken,
            };
            await putJsonKV(env.SESSION_KV, authKey, updatedAuth);
            await env.SESSION_KV.delete(changePasswordRateKey).catch(() => undefined);

            return new Response(JSON.stringify({
                success: true,
                token: newToken,
                expiresAt: newSession.expiresAt,
            }), { headers: baseHeaders });
        }

        if (apiPath === 'leaderboard' && request.method === 'GET') {
            const missingBindings = getMissingBindings(env, ['SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }

            const limitParam = url.searchParams.get('limit');
            const limit = limitParam ? parseInt(limitParam, 10) : 20;
            const bundle = await getLeaderboardBundle(env.SESSION_KV, isNaN(limit) ? 20 : limit);

            let me: { userId: string; username: string; stats: Awaited<ReturnType<typeof getUserStatsSummary>>; } | null = null;
            let maybeSession: SessionRecord | null = null;
            try {
                maybeSession = await getSessionFromRequest(request, env.SESSION_KV, false);
            } catch (sessionError: any) {
                if (sessionError?.status !== 401) {
                    throw sessionError;
                }
                maybeSession = null;
            }
            if (maybeSession) {
                const stats = await getUserStatsSummary(env.SESSION_KV, maybeSession.userId, maybeSession.username);
                me = { userId: maybeSession.userId, username: maybeSession.username, stats };
            }

            return new Response(JSON.stringify({
                ...bundle,
                me,
            }), { headers: baseHeaders });
        }

        // --- Route for "挑戰高考" --- (Requirement 5)
        if (apiPath === 'start_gaokao_set' && request.method === 'GET') {
            const missingBindings = getMissingBindings(env, ['SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }
            const session = await getSessionFromRequest(request, env.SESSION_KV, true);

            console.log("Processing /api/start_gaokao_set request using processed poems.json data");
            const poemData = getPreparedPoemData();

            if (poemData.allQuestions.length < MAX_QUESTIONS_PER_GAOKAO_SET) {
                throw new Error(`題庫中的總題目數量 (${poemData.allQuestions.length}) 不足 ${MAX_QUESTIONS_PER_GAOKAO_SET} 道，無法出題。`);
            }

            const setId = crypto.randomUUID();

            // Select random individual questions from the pre-flattened pool.
            const selectedQuestions = getRandomItems(poemData.allQuestions, MAX_QUESTIONS_PER_GAOKAO_SET);

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
            const newSet: QuestionSet = {
                setId,
                questions: generatedQuestions,
                createdAt: Date.now(),
                ownerUserId: session.userId,
            };
            try {
                await env.SESSION_KV.put(getGaokaoSetKey(setId), JSON.stringify(newSet), { expirationTtl: GAOKAO_SET_TTL_SECONDS });
                console.log(`Stored new GaoKao question set in KV with setId: ${setId} (${generatedQuestions.length} questions)`);
            } catch (kvError: any) {
                console.error(`KV put error for GaoKao setId ${setId}:`, kvError);
                throw new Error(`無法保存生成的題組信息: ${kvError.message}`);
            }

            // Prepare response for the frontend (without answers)
            const questionsForFrontend = newSet.questions.map(({ answer, ...rest }: QuestionInfo) => rest);
            return new Response(JSON.stringify({
                setId: newSet.setId,
                questions: questionsForFrontend,
                expiresAt: newSet.createdAt + (GAOKAO_SET_TTL_SECONDS * 1000),
                expiresInSeconds: GAOKAO_SET_TTL_SECONDS,
            }), { headers: baseHeaders });

        } // End /api/start_gaokao_set

        // --- Route to get chapter list for "選篇挑戰" --- (Requirement 6)
        if (apiPath === 'get_chapters' && request.method === 'GET') {
            const missingBindings = getMissingBindings(env, ['SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }
            await getSessionFromRequest(request, env.SESSION_KV, true);
            console.log("Processing /api/get_chapters request");
            const poemData = getPreparedPoemData();

            return new Response(JSON.stringify({ chapters: poemData.chapterList }), { headers: baseHeaders });
        }

        // --- Route to get questions for a specific chapter --- (Requirement 7)
        if (apiPath === 'get_chapter_questions' && request.method === 'GET') {
            const missingBindings = getMissingBindings(env, ['SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }
            await getSessionFromRequest(request, env.SESSION_KV, true);
            console.log(`Processing /api/get_chapter_questions request for order: ${chapterOrderParam}`);
            if (!chapterOrderParam) {
                return new Response(JSON.stringify({ error: '請求無效：缺少篇目順序號 (order)。' }), { status: 400, headers: baseHeaders });
            }
            const order = parseInt(chapterOrderParam, 10);
            if (isNaN(order)) {
                return new Response(JSON.stringify({ error: '請求無效：篇目順序號 (order) 必須是數字。' }), { status: 400, headers: baseHeaders });
            }

            const poemData = getPreparedPoemData();
            const chapterEntry = poemData.chapterMap.get(order);

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
            const missingBindings = getMissingBindings(env, ['IMAGES_BUCKET', 'GEMINI_API_KEY', 'SESSION_KV'] as const);
            if (missingBindings.length > 0) {
                return createMissingBindingsResponse(missingBindings, baseHeaders);
            }

            console.log("Processing /api/submit request");
            const session = await getSessionFromRequest(request, env.SESSION_KV, true);
            const userId = session.userId;
            const username = session.username;
            const submitLockKey = getSubmitLockKey(userId);
            const submitCooldownKey = getSubmitCooldownKey(userId);
            const submitLock = await env.SESSION_KV.get(submitLockKey);
            if (submitLock) {
                return new Response(JSON.stringify({ error: '上一份作答仍在處理中，請稍候。' }), { status: 429, headers: baseHeaders });
            }
            const submitCooldown = await env.SESSION_KV.get(submitCooldownKey);
            if (submitCooldown) {
                return new Response(JSON.stringify({ error: '提交過於頻繁，請稍後再試。' }), { status: 429, headers: baseHeaders });
            }
            await env.SESSION_KV.put(submitLockKey, Date.now().toString(), { expirationTtl: SUBMIT_LOCK_TTL_SECONDS });
            let gaokaoSetLockKey: string | null = null;
            let chapterDailyAttemptKey: string | null = null;
            let chapterWeeklyAttemptKey: string | null = null;
            let chapterDailyCount = 0;
            let chapterWeeklyCount = 0;
            let imageFingerprintKey: string | null = null;
            let imageFingerprintReserved = false;
            let imageFingerprintFinalized = false;
            let r2Key: string | null = null;
            let scoreCommitted = false;

            try {
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
            if (imageFile.size > MAX_UPLOAD_IMAGE_BYTES) {
                return new Response(JSON.stringify({ error: `圖片過大，請上傳不超過 ${Math.floor(MAX_UPLOAD_IMAGE_BYTES / 1024 / 1024)}MB 的圖片。` }), { status: 413, headers: baseHeaders });
            }
            console.log(`Validation passed for ${challengeType} challenge ${challengeIdentifier}. Image: ${imageFile.name}, Size: ${imageFile.size}, Type: ${imageFile.type}`);


            // --- Retrieve Correct Answers and Question Info ---
            let questionsToScore: QuestionInfo[] = [];
            let expectedQuestionCount = 0;
            let gaokaoSetStorageKey: string | null = null;

            if (challengeType === 'gaokao') {
                let questionSet: QuestionSet | null = null;
                gaokaoSetStorageKey = getGaokaoSetKey(challengeIdentifier);
                gaokaoSetLockKey = getGaokaoSetLockKey(challengeIdentifier);
                const setLock = await env.SESSION_KV.get(gaokaoSetLockKey);
                if (setLock) {
                    return new Response(JSON.stringify({ error: "此題組正在提交中，請勿重複提交。" }), { status: 409, headers: baseHeaders });
                }
                await env.SESSION_KV.put(gaokaoSetLockKey, Date.now().toString(), { expirationTtl: GAOKAO_SET_LOCK_TTL_SECONDS });

                try {
                    questionSet = await env.SESSION_KV.get<QuestionSet>(gaokaoSetStorageKey, 'json');
                } catch (kvError: any) {
                    console.error(`KV get error for GaoKao setId ${challengeIdentifier}:`, kvError);
                    return new Response(JSON.stringify({ error: "無法獲取“挑戰高考”題組信息，會話可能已過期或ID無效，請重新開始。" }), { status: 404, headers: baseHeaders });
                }
                if (!questionSet || !questionSet.questions || questionSet.questions.length === 0) {
                    console.error(`Invalid or missing/empty GaoKao question set data in KV for setId ${challengeIdentifier}`, questionSet);
                    return new Response(JSON.stringify({ error: "無效的“挑戰高考”題組信息，請重新開始。" }), { status: 400, headers: baseHeaders });
                }
                if (!questionSet.ownerUserId || questionSet.ownerUserId !== userId) {
                    return new Response(JSON.stringify({ error: "此題組不屬於當前登錄用戶，請重新開始挑戰。" }), { status: 403, headers: baseHeaders });
                }
                if (questionSet.usedAt) {
                    return new Response(JSON.stringify({ error: "此題組已提交過，請重新開始新的挑戰。" }), { status: 409, headers: baseHeaders });
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
                const chapterNow = Date.now();
                const chapterDailyKey = getDailyKey(chapterNow);
                const chapterWeeklyKey = getWeeklyKey(chapterNow);
                chapterDailyAttemptKey = getChapterAttemptKey('daily', chapterDailyKey, userId, order);
                chapterWeeklyAttemptKey = getChapterAttemptKey('weekly', chapterWeeklyKey, userId, order);
                const rawDailyCount = await env.SESSION_KV.get(chapterDailyAttemptKey);
                const rawWeeklyCount = await env.SESSION_KV.get(chapterWeeklyAttemptKey);
                chapterDailyCount = parseStoredCounter(rawDailyCount, CHAPTER_DAILY_SUBMIT_LIMIT);
                chapterWeeklyCount = parseStoredCounter(rawWeeklyCount, CHAPTER_WEEKLY_SUBMIT_LIMIT);
                if (chapterDailyCount >= CHAPTER_DAILY_SUBMIT_LIMIT) {
                    return new Response(JSON.stringify({ error: `本篇今日已達 ${CHAPTER_DAILY_SUBMIT_LIMIT} 次提交上限，請明日再試。` }), { status: 429, headers: baseHeaders });
                }
                if (chapterWeeklyCount >= CHAPTER_WEEKLY_SUBMIT_LIMIT) {
                    return new Response(JSON.stringify({ error: `本篇本週已達 ${CHAPTER_WEEKLY_SUBMIT_LIMIT} 次提交上限，請下週再試。` }), { status: 429, headers: baseHeaders });
                }
                const poemData = getPreparedPoemData();
                const chapterEntry = poemData.chapterMap.get(order);
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
            const imageFingerprint = await hashSha256Base64Url(imageBuffer);
            imageFingerprintKey = getImageFingerprintKey(userId, imageFingerprint);
            const hasUsedSameImage = await env.SESSION_KV.get(imageFingerprintKey);
            if (hasUsedSameImage) {
                return new Response(JSON.stringify({ error: '檢測到重複提交同一張圖片，請更換作答後再提交。' }), { status: 409, headers: baseHeaders });
            }
            await env.SESSION_KV.put(
                imageFingerprintKey,
                `pending:${Date.now()}`,
                { expirationTtl: IMAGE_FINGERPRINT_PENDING_TTL_SECONDS }
            );
            imageFingerprintReserved = true;
            r2Key = generateUniqueKey(`${challengeType}-${challengeIdentifier}-answer`, `.${imageFile.type.split('/')[1] || 'png'}`);
            try {
                await env.IMAGES_BUCKET.put(r2Key, imageBuffer, { httpMetadata: { contentType: imageFile.type } });
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
            const ocrPromptText = `这是一张包含${expectedQuestionCount}个手写简体中文答案的图片，按从上到下的顺序排列。请准确识别每个答案，并只用换行符（\\n）分隔返回${expectedQuestionCount}个结果。注意：圖片是學生提交的默寫考試內容，識別過程中務必保持中文字形原貌，絕對不要修正或添加任何其他文字、解释、编号或格式。如果筆畫不清晰要直接視為錯誤答案。如果字形有錯誤直接視為錯誤答案。識別過程中絕對不要做語意分析，審閱預期中要保持學生有很大概率會寫錯的警惕心。如果某个答案无法识别，请在那一行输出 "[無法識別]"。`;
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
            const scoreTarget = getScoreTarget(expectedQuestionCount);
            const pointsPerQuestion = expectedQuestionCount > 0 ? (scoreTarget / expectedQuestionCount) : 0;

            function removePunctuation(text: string): string {
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
                let success = !recognized.startsWith("[");
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
            console.log(`Scoring complete for ${challengeType} challenge ${challengeIdentifier}. Total score: ${totalScore} / ${scoreTarget}`);

            // --- Generate Feedback using AI ---
            let feedback = "";
            const feedbackStartTime = Date.now();
            let feedbackErrorMsg: string | null = null;
            let feedbackFinishReason: string | null = null;

            // --- Generate Feedback Text ---
            // Same prompt logic, adapted score context
            if (totalScore === scoreTarget) {
                feedback = `太棒了！滿分 ${scoreTarget} 分！簡直是默寫的神！繼續保持！`;
                feedbackErrorMsg = null;

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

            const pointsAwarded = calculatePointsAwarded(totalScore, scoreTarget, expectedQuestionCount);
            const now = Date.now();
            if (imageFingerprintKey) {
                await env.SESSION_KV.put(imageFingerprintKey, String(now), { expirationTtl: IMAGE_FINGERPRINT_TTL_SECONDS });
                imageFingerprintFinalized = true;
            }
            if (challengeType === 'gaokao' && gaokaoSetStorageKey) {
                try {
                    await env.SESSION_KV.delete(gaokaoSetStorageKey);
                } catch (kvDeleteError: any) {
                    console.error(`Failed to delete used GaoKao set ${gaokaoSetStorageKey}:`, kvDeleteError);
                    throw new Error('題組狀態更新失敗，請稍後再試。');
                }
            }
            const dailyKey = getDailyKey(now);
            const weeklyKey = getWeeklyKey(now);
            const [totalStats, weeklyStats, dailyStats] = await Promise.all([
                updateScopedStats(env.SESSION_KV, 'total', 'all', userId, username, pointsAwarded, totalScore),
                updateScopedStats(env.SESSION_KV, 'weekly', weeklyKey, userId, username, pointsAwarded, totalScore),
                updateScopedStats(env.SESSION_KV, 'daily', dailyKey, userId, username, pointsAwarded, totalScore),
            ]);
            scoreCommitted = true;
            if (challengeType === 'chapter' && chapterDailyAttemptKey && chapterWeeklyAttemptKey) {
                await Promise.all([
                    env.SESSION_KV.put(chapterDailyAttemptKey, String(chapterDailyCount + 1), { expirationTtl: CHAPTER_DAILY_COUNTER_TTL_SECONDS }),
                    env.SESSION_KV.put(chapterWeeklyAttemptKey, String(chapterWeeklyCount + 1), { expirationTtl: CHAPTER_WEEKLY_COUNTER_TTL_SECONDS }),
                ]);
            }
            let leaderboardSnapshot: Awaited<ReturnType<typeof getLeaderboardBundle>> | null = null;
            try {
                await bumpLeaderboardVersion(env.SESSION_KV);
                leaderboardSnapshot = await getLeaderboardBundle(env.SESSION_KV, 10);
            } catch (leaderboardError: any) {
                console.error(`Leaderboard snapshot generation failed after submit for user ${userId}:`, leaderboardError?.message || leaderboardError);
                leaderboardSnapshot = null;
            }
            await env.SESSION_KV.put(submitCooldownKey, Date.now().toString(), { expirationTtl: SUBMIT_COOLDOWN_SECONDS });

            const responseData: any = { // Use 'any' temporarily for flexibility
                message: finalMessage,
                totalScore: totalScore,
                scoreTarget: scoreTarget, // Send the target score back
                results: results,
                feedback: feedback,
                ocrIssue: ocrError,
                feedbackIssue: feedbackErrorMsg,
                pointsAwarded,
                user: {
                    userId,
                    username,
                },
                userStats: {
                    total: toLeaderboardEntry(totalStats),
                    weekly: toLeaderboardEntry(weeklyStats),
                    daily: toLeaderboardEntry(dailyStats),
                },
                leaderboardSnapshot
            };

            return new Response(JSON.stringify(responseData), { headers: baseHeaders });
            } finally {
                if (!scoreCommitted) {
                    if (imageFingerprintKey && imageFingerprintReserved && !imageFingerprintFinalized) {
                        await env.SESSION_KV.delete(imageFingerprintKey).catch((cleanupError: any) => {
                            console.warn(`Failed to clear pending image fingerprint ${imageFingerprintKey}:`, cleanupError?.message || cleanupError);
                        });
                    }
                    if (r2Key) {
                        await env.IMAGES_BUCKET.delete(r2Key).catch((cleanupError: any) => {
                            console.warn(`Failed to remove R2 object after failed submit ${r2Key}:`, cleanupError?.message || cleanupError);
                        });
                    }
                }
                await env.SESSION_KV.delete(submitLockKey).catch((cleanupError: any) => {
                    console.warn(`Failed to clear submit lock ${submitLockKey}:`, cleanupError?.message || cleanupError);
                });
                if (gaokaoSetLockKey) {
                    await env.SESSION_KV.delete(gaokaoSetLockKey).catch((cleanupError: any) => {
                        console.warn(`Failed to clear GaoKao set lock ${gaokaoSetLockKey}:`, cleanupError?.message || cleanupError);
                    });
                }
            }
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
