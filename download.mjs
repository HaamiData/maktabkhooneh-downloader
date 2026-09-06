/**
 * CLI tool to download all lecture videos of a maktabkhooneh course
 * 
 * Usage examples:
 *   node download.mjs "https://maktabkhooneh.org/course/<slug>/" --user you@example.com --pass "Secret123"
 *   node download.mjs "https://maktabkhooneh.org/course/<slug>/" --sample-bytes 65536 --verbose
 * 
 * Notes: Only download content you have legal rights to access.
 * 
 * @repository https://github.com/HaamiData/maktabkhooneh-downloader
 * @maintainer HaamiData <https://haamidata.ir>
 * @license GPL-3.0
 * @created 2025
 * 
 * This is a fork with substantial changes (auto-detection of maktabkhooneh's
 * new LMS API, quality selection, parallel downloads, chapter filtering,
 * CSV/HTML reporting, and more) maintained by HaamiData.
 * Originally created by NabiKAZ <https://github.com/NabiKAZ>.
 * 
 * Copyright(C) 2025 NabiKAZ
 * Copyright(C) 2025 HaamiData (modifications)
 */

import fs from 'fs';
import path from 'path';
import { Transform, Readable } from 'stream';
import { pipeline } from 'stream/promises';
import { setTimeout as sleep } from 'timers/promises';
import readline from 'readline/promises';

// ===============
// Console styling (ANSI colors) and emojis
// ===============
const COLOR = {
    reset: '\u001b[0m', bold: '\u001b[1m', dim: '\u001b[2m',
    red: '\u001b[31m', green: '\u001b[32m', yellow: '\u001b[33m', blue: '\u001b[34m', magenta: '\u001b[35m', cyan: '\u001b[36m',
    lightBlue: '\u001b[94m'
};
const paint = (code, s) => `${code}${s}${COLOR.reset}`;
const paintBold = s => paint(COLOR.bold, s);
const paintGreen = s => paint(COLOR.green, s);
const paintRed = s => paint(COLOR.red, s);
const paintYellow = s => paint(COLOR.yellow, s);
const paintCyan = s => paint(COLOR.cyan, s);
// Combined style helpers
const paintBoldCyan = s => `${COLOR.bold}${COLOR.cyan}${s}${COLOR.reset}`; // bold + cyan
const paintBlue = s => paint(COLOR.blue, s);
const paintLightBlue = s => paint(COLOR.lightBlue, s);

const logInfo = (...a) => console.log('ℹ️', ...a);
const logStep = (...a) => console.log('▶️', ...a);
const logSuccess = (...a) => console.log('✅', ...a);
const logWarn = (...a) => console.warn('⚠️', ...a);
const logError = (...a) => console.error('❌', ...a);

// ===============
// Optional log-file mirroring (enabled by default, disabled via --no-log)
// ===============
let logFileStream = null;
function stripAnsi(s) { return String(s).replace(/\u001b\[[0-9;]*m/g, ''); }
function setupLogFile(logFilePath) {
    try {
        fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        logFileStream = fs.createWriteStream(logFilePath, { flags: 'a' });
        const origLog = console.log.bind(console);
        const origWarn = console.warn.bind(console);
        const origError = console.error.bind(console);
        const writeLine = (parts) => {
            try { logFileStream.write(`[${new Date().toISOString()}] ${parts.map(stripAnsi).join(' ')}\n`); } catch { }
        };
        console.log = (...a) => { origLog(...a); writeLine(a); };
        console.warn = (...a) => { origWarn(...a); writeLine(a); };
        console.error = (...a) => { origError(...a); writeLine(a); };
        return true;
    } catch (e) {
        logWarn('Could not open log file: ' + e.message);
        return false;
    }
}

// ===============
// Configuration
// ===============
// Cookie: read from env MK_COOKIE or file path in MK_COOKIE_FILE; fallback to placeholder.
const COOKIE = (() => {
    if (process.env.MK_COOKIE && process.env.MK_COOKIE.trim()) return process.env.MK_COOKIE.trim();
    if (process.env.MK_COOKIE_FILE) {
        try { return fs.readFileSync(process.env.MK_COOKIE_FILE, 'utf8').trim(); } catch { }
    }
    return 'PUT_YOUR_COOKIE_HERE';
})();
// ACTIVE_COOKIE will be dynamically set after login/session load (fallback to COOKIE)
let ACTIVE_COOKIE = null;
// Sample mode default (0 means full download)
const DEFAULT_SAMPLE_BYTES = 0;
// Detected per-course API mode: null (not yet determined) | 'new' (LMS JSON API) | 'old' (HTML scrape)
// Determined lazily from the first unit that resolves unambiguously, then reused for the rest of the course.
let courseApiMode = null;

// Ensure Node 18+ for global fetch
if (typeof fetch !== 'function') {
    logError('This script requires Node.js v18+ with global fetch.');
    process.exit(1);
}

const ORIGIN = 'https://maktabkhooneh.org';

// Extract the csrftoken value out of a raw "k=v; k2=v2; ..." cookie header string.
function extractCsrfTokenFromCookie(cookieStr) {
    if (!cookieStr) return null;
    const m = cookieStr.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return m ? m[1].trim() : null;
}

// Build common headers for authenticated requests.
function commonHeaders(referer) {
    /** @type {Record<string,string>} */
    const headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,fa;q=0.8',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'x-requested-with': 'XMLHttpRequest',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36',
    };
    const ck = ACTIVE_COOKIE || COOKIE;
    if (ck && ck !== 'PUT_YOUR_COOKIE_HERE') {
        headers['cookie'] = ck;
        // The site now enforces a Django-style CSRF check even on GET API calls;
        // the header value must mirror the csrftoken cookie value.
        const csrf = extractCsrfTokenFromCookie(ck);
        if (csrf) headers['x-csrftoken'] = csrf;
    }
    if (referer) headers['referer'] = referer;
    return headers;
}

// Human-friendly byte formatter
function formatBytes(bytes) {
    if (bytes == null || isNaN(bytes)) return '-';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0; let n = Number(bytes);
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return `${n.toFixed(n >= 100 ? 0 : n >= 10 ? 1 : 2)} ${units[i]}`;
}

function formatSpeed(bytesPerSec) {
    if (!bytesPerSec || !isFinite(bytesPerSec)) return '-';
    return `${formatBytes(bytesPerSec)}/s`;
}

function buildProgressBar(ratio, width = 24) {
    const r = Math.max(0, Math.min(1, ratio || 0));
    const filled = Math.round(r * width);
    const left = width - filled;
    const bar = `${'█'.repeat(filled)}${'░'.repeat(left)}`;
    return bar;
}

function ensureCookiePresent() {
    if (!(ACTIVE_COOKIE && ACTIVE_COOKIE !== 'PUT_YOUR_COOKIE_HERE') && !(COOKIE && COOKIE !== 'PUT_YOUR_COOKIE_HERE')) {
        logError('No active session. Provide --user / --pass to login or set MK_COOKIE / MK_COOKIE_FILE.');
        process.exit(1);
    }
}

// CLI usage
function printUsage() {
    // Header section
    console.log(`${paintBoldCyan('Maktabkhooneh Downloader')} - ${paintYellow('version 2.0.0')} ${paint(COLOR.dim, '© 2025')}`);
    console.log(paint(COLOR.magenta, 'Maintained by ') + paint(COLOR.magenta, 'HaamiData') + ' ' + paintLightBlue('<haamidata.ir>'));
    console.log(paint(COLOR.dim, 'Originally created by NabiKAZ '));
    console.log(paint(COLOR.dim, 'Signup: ') + paintLightBlue('https://maktabkhooneh.org/'));
    console.log(paint(COLOR.dim, 'Project: ') + paintLightBlue('https://github.com/HaamiData/maktabkhooneh-downloader'));
    console.log(paint(COLOR.dim, '=============================================================\n'));

    // Usage
    console.log(paintBold('Usage:'));
    console.log(`  ${paintCyan('node download.mjs')} ${paintYellow('<course_url>')} [options]`);

    // Options
    console.log('\n' + paintBold('Options:'));
    console.log(`  ${paintYellow('<course_url>')}                The maktabkhooneh course URL (old /course/<slug>/ or new /lms/course/<slug>/unit/<id>/)`);
    console.log(`  ${paintGreen('--sample-bytes')} ${paintYellow('N')}            Download only the first N bytes of each video (also via env MK_SAMPLE_BYTES)`);
    console.log(`  ${paintGreen('--user')} | ${paintGreen('--email')} ${paintYellow('<EMAIL>')}    Login with email (stores cookie in session file)`);
    console.log(`  ${paintGreen('--pass')} | ${paintGreen('--password')} ${paintYellow('<PASS>')}  Password for login (consider quoting)`);
    console.log(`  ${paintGreen('--session-file')} ${paintYellow('<FILE>')}       Session store path (default: session.json, multi-user)`);
    console.log(`  ${paintGreen('--force-login')}               Force fresh login even if stored session is valid`);
    console.log(`  ${paintGreen('--quality')} ${paintYellow('<1080p|720p|...|best|worst>')}  Preferred video quality (default: best; falls back to nearest lower)`);
    console.log(`  ${paintGreen('--parallel')} ${paintYellow('<1-3>')}           Number of simultaneous downloads (default: 1, sequential)`);
    console.log(`  ${paintGreen('--chapters')} ${paintYellow('<3-5|2,4,7>')}     Only download specific chapter(s) by index`);
    console.log(`  ${paintGreen('--no-subtitle')}               Skip downloading subtitles`);
    console.log(`  ${paintGreen('--no-attachments')}            Skip downloading lecture attachments`);
    console.log(`  ${paintGreen('--no-log')}                    Disable the download.log file (enabled by default)`);
    console.log(`  ${paintGreen('--estimate-size')}             Estimate total course size before downloading (disabled by default)`);
    console.log(`  ${paintGreen('--dry-run')}                   List what would be downloaded (with a CSV) without downloading anything`);
    console.log(`  ${paintGreen('--retry-failed')}              Only retry units marked "failed" in the last report.csv for this course`);
    console.log(`  ${paintGreen('--verify-integrity')}          After downloading, verify file sizes against the server and offer to re-download mismatches`);
    console.log(`  ${paintGreen('--output')} ${paintYellow('<path>')}              Custom output directory (default: ./download/<courseName>)`);
    console.log(`  ${paintGreen('--no-wake-lock')}              (Termux/Android) Skip acquiring a wake-lock during download`);
    console.log(`  ${paintGreen('--no-notify')}                 (Termux/Android) Skip the end-of-run notification`);
    console.log(`  ${paintGreen('--verbose')} | ${paintGreen('-v')}              Verbose debug / HTTP flow info`);
    console.log(`  ${paintGreen('--help')} | ${paintGreen('-h')}                 Show this help and exit`);
    console.log('\n' + paintBold('Env vars:'));
    console.log(`    MK_COOKIE / MK_COOKIE_FILE   Override cookie manually (bypass credential login)`);
    console.log(`    MK_SAMPLE_BYTES              Default sample bytes (overridden by --sample-bytes)`);

    // Examples
    console.log('\n' + paintBold('Examples:'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/"'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --sample-bytes 65536 --verbose'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --user you@example.com --pass "Secret123"'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --user you@example.com --pass "Secret123" --force-login'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --quality 720p --parallel 3'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --chapters 3-5 --no-subtitle'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --dry-run'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --retry-failed'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --verify-integrity'));
    console.log('  ' + paintCyan('node download.mjs "https://maktabkhooneh.org/course/<slug>/" --output ~/storage/downloads/Maktabkhooneh'));
    console.log('');
}

function parseCLI() {
    const args = process.argv.slice(2);
    let inputCourseUrl = null;
    let sampleBytesToDownload = DEFAULT_SAMPLE_BYTES;
    let isVerboseLoggingEnabled = false;
    let userEmail = null;
    let userPassword = null;
    let sessionFile = 'session.json';
    let forceLogin = false;
    let quality = 'best';
    let parallel = 1;
    let chaptersSpec = null;
    let noSubtitle = false;
    let noAttachments = false;
    let noLog = false;
    let estimateSize = false;
    let dryRun = false;
    let retryFailed = false;
    let verifyIntegrity = false;
    let outputDir = null;
    let noWakeLock = false;
    let noNotify = false;
    for (let i = 0; i < args.length; i++) {
        const a = args[i];
        if (a === '--help' || a === '-h') {
            printUsage();
            process.exit(0);
        } else if (a === '--user' || a === '--email') {
            const v = args[i + 1]; if (v) { userEmail = v; i++; }
        } else if (a.startsWith('--user=')) {
            userEmail = a.split('=')[1];
        } else if (a === '--pass' || a === '--password') {
            const v = args[i + 1]; if (v) { userPassword = v; i++; }
        } else if (a.startsWith('--pass=')) {
            userPassword = a.split('=')[1];
        } else if (a === '--session-file') {
            const v = args[i + 1]; if (v) { sessionFile = v; i++; }
        } else if (a.startsWith('--session-file=')) {
            sessionFile = a.split('=')[1];
        } else if (a.startsWith('--sample-bytes=')) {
            const v = a.split('=')[1];
            sampleBytesToDownload = parseInt(v, 10) || 0;
        } else if (a === '--sample-bytes') {
            const v = args[i + 1];
            if (v) { sampleBytesToDownload = parseInt(v, 10) || 0; i++; }
        } else if (a === '--verbose' || a === '-v') {
            isVerboseLoggingEnabled = true;
        } else if (a === '--force-login') {
            forceLogin = true;
        } else if (a === '--quality') {
            const v = args[i + 1]; if (v) { quality = parseQualityArg(v); i++; }
        } else if (a.startsWith('--quality=')) {
            quality = parseQualityArg(a.split('=')[1]);
        } else if (a === '--parallel') {
            const v = args[i + 1]; if (v) { parallel = Math.max(1, Math.min(3, parseInt(v, 10) || 1)); i++; }
        } else if (a.startsWith('--parallel=')) {
            parallel = Math.max(1, Math.min(3, parseInt(a.split('=')[1], 10) || 1));
        } else if (a === '--chapters') {
            const v = args[i + 1]; if (v) { chaptersSpec = v; i++; }
        } else if (a.startsWith('--chapters=')) {
            chaptersSpec = a.split('=')[1];
        } else if (a === '--no-subtitle') {
            noSubtitle = true;
        } else if (a === '--no-attachments') {
            noAttachments = true;
        } else if (a === '--no-log') {
            noLog = true;
        } else if (a === '--estimate-size') {
            estimateSize = true;
        } else if (a === '--dry-run') {
            dryRun = true;
        } else if (a === '--retry-failed') {
            retryFailed = true;
        } else if (a === '--verify-integrity') {
            verifyIntegrity = true;
        } else if (a === '--output') {
            const v = args[i + 1]; if (v) { outputDir = v; i++; }
        } else if (a.startsWith('--output=')) {
            outputDir = a.split('=')[1];
        } else if (a === '--no-wake-lock') {
            noWakeLock = true;
        } else if (a === '--no-notify') {
            noNotify = true;
        } else if (!inputCourseUrl) {
            inputCourseUrl = a;
        }
    }
    if (!sampleBytesToDownload && process.env.MK_SAMPLE_BYTES) {
        sampleBytesToDownload = parseInt(process.env.MK_SAMPLE_BYTES, 10) || 0;
    }
    return {
        inputCourseUrl, sampleBytesToDownload, isVerboseLoggingEnabled, userEmail, userPassword, sessionFile, forceLogin,
        quality, parallel, chaptersSpec, noSubtitle, noAttachments, noLog, estimateSize, dryRun, retryFailed, verifyIntegrity,
        outputDir, noWakeLock, noNotify
    };
}

function createVerboseLogger(isVerbose) {
    return { verbose: (...a) => { if (isVerbose) console.log(...a); } };
}

// Parse a --chapters spec like "3-5" or "2,4,7" or "1-3,5,9-10" into a Set of 1-based chapter indices.
// Returns null when spec is empty/omitted (meaning "all chapters").
function parseChapterSpec(spec) {
    if (!spec) return null;
    const result = new Set();
    for (const part of String(spec).split(',').map(s => s.trim()).filter(Boolean)) {
        const rangeMatch = part.match(/^(\d+)\s*-\s*(\d+)$/);
        if (rangeMatch) {
            let a = parseInt(rangeMatch[1], 10), b = parseInt(rangeMatch[2], 10);
            if (a > b) [a, b] = [b, a];
            for (let i = a; i <= b; i++) result.add(i);
        } else {
            const n = parseInt(part, 10);
            if (!isNaN(n)) result.add(n);
        }
    }
    return result.size ? result : null;
}

// Parse the course slug from the full course URL.
function extractCourseSlug(courseUrl) {
    try {
        const parsed = new URL(courseUrl);
        if (parsed.origin !== ORIGIN) {
            throw new Error('Unexpected origin: ' + parsed.origin);
        }
        const parts = parsed.pathname.split('/').filter(Boolean);
        const idx = parts.indexOf('course');
        if (idx === -1 || !parts[idx + 1]) throw new Error('Cannot parse course slug');
        return parts[idx + 1];
    } catch (e) {
        throw new Error('Invalid course URL: ' + e.message);
    }
}

// Fetch with timeout.
async function fetchWithTimeout(url, options = {}, timeoutMs = 60_000) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const res = await fetch(url, { ...options, signal: controller.signal });
        return res;
    } finally {
        clearTimeout(t);
    }
}

function ensureTrailingSlash(u) { return u.endsWith('/') ? u : u + '/'; }

// Fetch wrapper that transparently retries on HTTP 429 (Too Many Requests),
// honoring the Retry-After header when present, falling back to capped exponential backoff.
async function fetchWithRetry429(url, options = {}, timeoutMs = 60_000, max429Retries = 5) {
    for (let attempt = 0; attempt <= max429Retries; attempt++) {
        const res = await fetchWithTimeout(url, options, timeoutMs);
        if (res.status !== 429) return res;
        const retryAfterHeader = res.headers.get('retry-after');
        let waitMs = null;
        if (retryAfterHeader) {
            const asSeconds = Number(retryAfterHeader);
            if (!isNaN(asSeconds)) waitMs = asSeconds * 1000;
            else {
                const asDate = Date.parse(retryAfterHeader);
                if (!isNaN(asDate)) waitMs = Math.max(0, asDate - Date.now());
            }
        }
        if (waitMs == null) waitMs = Math.min(30_000, 2000 * Math.pow(2, attempt));
        try { if (res.body) { const rb = Readable.fromWeb(res.body); rb.resume(); } } catch { }
        if (attempt < max429Retries) {
            logWarn(`⏳ HTTP 429 Too Many Requests — waiting ${Math.round(waitMs / 1000)}s before retry (${attempt + 1}/${max429Retries}) — ${url}`);
            await sleep(waitMs);
        } else {
            return res; // give up, return the 429 response for the caller to handle/report
        }
    }
}

// Try to detect remote file size and whether server supports Range
async function getRemoteSizeAndRanges(url, referer) {
    // HEAD first
    try {
        const res = await fetchWithTimeout(url, { method: 'HEAD', headers: { ...commonHeaders(referer), accept: '*/*' } }, 20_000);
        if (res.ok) {
            const len = res.headers.get('content-length');
            const size = len ? parseInt(len, 10) : undefined;
            const acceptRanges = (res.headers.get('accept-ranges') || '').toLowerCase().includes('bytes');
            return { size, acceptRanges };
        }
    } catch { }
    // Fallback: GET single byte
    try {
        const res = await fetchWithTimeout(url, { method: 'GET', headers: { ...commonHeaders(referer), range: 'bytes=0-0', accept: '*/*' } }, 20_000);
        if (res.status === 206) {
            const cr = res.headers.get('content-range');
            // e.g. bytes 0-0/123456
            const m = cr && cr.match(/\/(\d+)$/);
            const size = m ? parseInt(m[1], 10) : undefined;
            try { if (res.body) { const rb = Readable.fromWeb(res.body); rb.resume(); } } catch { }
            return { size, acceptRanges: true };
        }
    } catch { }
    return { size: undefined, acceptRanges: false };
}

// API: fetch chapters JSON for a course.
async function fetchChapters(courseSlug, referer) {
    const apiUrl = `${ORIGIN}/api/v1/courses/${courseSlug}/chapters/`;
    const res = await fetchWithRetry429(apiUrl, { method: 'GET', headers: { ...commonHeaders(referer), accept: 'application/json' } });
    if (!res.ok) throw new Error(`Failed to fetch chapters: ${res.status} ${res.statusText}`);
    return res.json();
}

// API: core-data to verify authentication and basic profile.
async function fetchCoreData(referer) {
    const url = `${ORIGIN}/api/v1/general/core-data/?profile=1`;
    const res = await fetchWithRetry429(url, { method: 'GET', headers: { ...commonHeaders(referer || ORIGIN), accept: 'application/json' } }, 30_000);
    if (!res.ok) throw new Error(`Core-data request failed: ${res.status} ${res.statusText}`);
    return res.json();
}

function printProfileSummary(core) {
    const isAuthenticated = !!core?.auth?.details?.is_authenticated;
    const email = core?.auth?.details?.email || core?.profile?.details?.email || '-';
    const userId = core?.auth?.details?.user_id ?? '-';
    const studentId = core?.auth?.details?.student_id ?? '-';
    const hasSubscription = !!core?.auth?.conditions?.has_subscription;
    const hasCoursePurchase = !!core?.auth?.conditions?.has_course_purchase;
    const statusText = isAuthenticated ? paintGreen('Authenticated') : paintRed('NOT authenticated');
    console.log(`🔐 Auth check: ${statusText}`);
    console.log(`👤 User: ${paintCyan(email)}  | user_id: ${paintCyan(userId)}  | student_id: ${paintCyan(studentId)}`);
    console.log(`💳 Subscription: ${hasSubscription ? paintGreen('yes') : paintYellow('no')}  | Has course purchase: ${hasCoursePurchase ? paintGreen('yes') : paintYellow('no')}`);
    return isAuthenticated;
}

// Build lecture page URL for a specific chapter/unit.
function buildLectureUrl(courseSlug, chapter, unit) {
    const chapterSegment = `${encodeURIComponent(chapter.slug)}-ch${chapter.id}`;
    const unitSegment = encodeURIComponent(unit.slug);
    return `${ORIGIN}/course/${courseSlug}/${chapterSegment}/${unitSegment}/`;
}

// Minimal HTML entities decoder for attribute values.
function decodeHtmlEntities(str) {
    if (!str) return str;
    return str
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;|&apos;/g, "'")
        .replace(/&#(\d+);/g, (_, d) => String.fromCharCode(parseInt(d, 10)))
        .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)));
}

// Extract <source ... src="..."> URLs from lecture page HTML.
function extractVideoSources(html) {
    const urls = [];
    const re = /<source\b[^>]*?src=["']([^"'>]+)["'][^>]*>/gim;
    let m;
    while ((m = re.exec(html)) !== null) {
        const raw = m[1];
        const url = decodeHtmlEntities(raw);
        if (url && url.includes('/videos/')) urls.push(url);
    }
    return Array.from(new Set(urls));
}

// Parse a --quality value into a normalized token: 'best' | 'worst' | '<N>p'
function parseQualityArg(v) {
    if (!v) return 'best';
    const s = String(v).trim().toLowerCase();
    if (s === 'best' || s === 'worst') return s;
    const m = s.match(/(\d+)/);
    return m ? `${m[1]}p` : 'best';
}

function qualityToNumber(q) {
    const m = /^(\d+)p?$/.exec(q || '');
    return m ? parseInt(m[1], 10) : null;
}

// Pick a source URL from old-style HTML <source> URLs, honoring --quality as best as this
// format allows. Old sources don't carry explicit resolution metadata; only an "hq" tag can be
// detected reliably, so any specific numeric quality request downgrades to a non-hq source if
// one exists (best-effort — flagged via `downgraded`).
function pickSourceForQuality(urls, desiredQuality) {
    if (!urls || urls.length === 0) return { url: null, downgraded: false, label: null };
    const hq = urls.find(u => /\/videos\/hq\d+/.test(u) || u.includes('/videos/hq'));
    const others = urls.filter(u => u !== hq);
    if (!desiredQuality || desiredQuality === 'best') return { url: hq || urls[0], downgraded: false, label: hq ? 'hq' : 'default' };
    if (desiredQuality === 'worst') return { url: others[0] || hq || urls[0], downgraded: false, label: others[0] ? 'lq' : 'hq' };
    // Specific resolution requested but old sources can't be matched by resolution.
    if (others.length) return { url: others[0], downgraded: true, label: 'lq (approx.)' };
    return { url: hq || urls[0], downgraded: true, label: 'hq (only option)' };
}

// Pick a quality-matched video URL from the new /video_url/ JSON response.
// Exact resolution match preferred; falls back to the nearest LOWER resolution; if none lower
// exists, falls back to the lowest available (still flagged as downgraded).
function pickVideoUrlForQuality(videoJson, desiredQuality) {
    if (!videoJson) return { url: null, downgraded: false, label: null };
    const list = Array.isArray(videoJson.qualities) ? videoJson.qualities : [];
    if (list.length > 0) {
        const sorted = [...list].sort((a, b) => (b.resolution || 0) - (a.resolution || 0));
        let chosen = null;
        let downgraded = false;
        if (!desiredQuality || desiredQuality === 'best') {
            chosen = sorted[0];
        } else if (desiredQuality === 'worst') {
            chosen = sorted[sorted.length - 1];
        } else {
            const wantRes = qualityToNumber(desiredQuality);
            chosen = wantRes != null ? sorted.find(q => q.resolution === wantRes) : null;
            if (!chosen && wantRes != null) {
                const lower = sorted.filter(q => (q.resolution || 0) <= wantRes);
                if (lower.length) { chosen = lower[0]; downgraded = true; }
            }
            if (!chosen) { chosen = sorted[sorted.length - 1]; downgraded = true; }
        }
        return { url: chosen?.download_url || null, downgraded, label: chosen?.quality || null };
    }
    // Fallback to hq/lq shortcuts when no detailed qualities[] array is present.
    if (videoJson.video_urls?.hq || videoJson.video_urls?.lq) {
        if (desiredQuality === 'worst' && videoJson.video_urls?.lq) {
            return { url: videoJson.video_urls.lq, downgraded: false, label: 'lq' };
        }
        const usedHq = !!videoJson.video_urls?.hq;
        return {
            url: videoJson.video_urls.hq || videoJson.video_urls.lq,
            downgraded: !usedHq && desiredQuality && desiredQuality !== 'best',
            label: usedHq ? 'hq' : 'lq'
        };
    }
    return { url: null, downgraded: false, label: null };
}

// Sanitize a string for safe Windows filenames.
function sanitizeName(name) {
    return name.replace(/[\/:*?"<>|]/g, ' ').replace(/[\s\u200c\u200f\u202a\u202b]+/g, ' ').trim().slice(0, 150);
}

// Extract attachment links from lecture HTML.
function extractAttachmentLinks(html) {
    const results = new Set();
    if (!html) return [];
    // Regex to capture <div class="...unit-content--download..."> ... <a href="..."> inside
    const blockRe = /<div[^>]*class=["'][^"'>]*unit-content--download[^"'>]*["'][^>]*>[\s\S]*?<\/div>/gim;
    let m;
    while ((m = blockRe.exec(html)) !== null) {
        const block = m[0];
        // Find anchor hrefs inside this block
        const aRe = /<a[^>]+href=["']([^"'>]+)["'][^>]*>/gim;
        let a;
        while ((a = aRe.exec(block)) !== null) {
            const raw = a[1];
            const url = decodeHtmlEntities(raw);
            if (url && /attachments/i.test(url)) {
                results.add(url);
            }
        }
    }
    return Array.from(results);
}

// ===============
// New LMS API (maktabkhooneh moved unit pages to /lms/course/.../unit/<id>/,
// content is loaded client-side via JSON, not embedded in HTML anymore)
// ===============

// GET /api/v1/lms/units/<unitId>/  -> title, description, resources[], caption_file, has_caption, ...
async function fetchUnitDetailNew(unitId, referer) {
    const url = `${ORIGIN}/api/v1/lms/units/${unitId}/`;
    const res = await fetchWithRetry429(url, { headers: { ...commonHeaders(referer), accept: 'application/json' } }, 30_000);
    return res;
}

// GET /api/v1/lms/units/<unitId>/video_url/  -> qualities[], hls, video_urls.hq/lq
async function fetchUnitVideoUrlNew(unitId, referer) {
    const url = `${ORIGIN}/api/v1/lms/units/${unitId}/video_url/`;
    const res = await fetchWithRetry429(url, { headers: { ...commonHeaders(referer), accept: 'application/json' } }, 30_000);
    return res;
}

// Best-effort extraction of attachment-like entries from the new unit-detail JSON.
// The exact field name for attachments hasn't been confirmed on a real sample yet,
// so this checks a few plausible keys and silently returns [] if none match
// (rather than guessing wrong and breaking on an unexpected shape).
function extractNewAttachments(unitDetailJson) {
    if (!unitDetailJson) return [];
    const candidateArrays = [unitDetailJson.attachments, unitDetailJson.files, unitDetailJson.downloads]
        .filter(a => Array.isArray(a) && a.length > 0);
    if (candidateArrays.length === 0) return [];
    const arr = candidateArrays[0];
    return arr
        .map(item => ({
            url: item.download_url || item.url || item.file || null,
            name: item.title || item.display_title || item.name || null
        }))
        .filter(a => !!a.url);
}

// Try the new LMS API for a given unit.
// Returns one of:
//   { ok: true, videoUrl, unitDetail, qualityLabel, downgraded }
//   { ok: false, reason: 'not-found' }   -> this course/unit doesn't have the new endpoint (old course)
//   { ok: false, reason: 'locked' }      -> endpoint exists but access denied (401/403)
//   { ok: false, reason: 'no-video' }    -> endpoint exists, responded OK, but had no usable video url
//   { ok: false, reason: 'error', status }
async function tryNewApiForUnit(unit, referer, desiredQuality = 'best') {
    let res;
    try {
        res = await fetchUnitVideoUrlNew(unit.id, referer);
    } catch (e) {
        return { ok: false, reason: 'error', status: null, message: e.message };
    }
    if (res.status === 404) return { ok: false, reason: 'not-found', status: 404 };
    if (res.status === 401 || res.status === 403) return { ok: false, reason: 'locked', status: res.status };
    if (!res.ok) return { ok: false, reason: 'error', status: res.status };

    let videoJson = null;
    try { videoJson = await res.json(); } catch { return { ok: false, reason: 'error', message: 'invalid JSON from video_url' }; }
    const picked = pickVideoUrlForQuality(videoJson, desiredQuality);
    if (!picked.url) return { ok: false, reason: 'no-video' };

    // Unit detail (description, caption, possible attachments) - non-fatal if it fails.
    let unitDetail = null;
    try {
        const dRes = await fetchUnitDetailNew(unit.id, referer);
        if (dRes.ok) unitDetail = await dRes.json();
    } catch { }

    return { ok: true, videoUrl: picked.url, unitDetail, qualityLabel: picked.label, downgraded: picked.downgraded };
}

// --- Session / Login helpers ---
// --- Multi-user session file helpers ---
// Structure:
// {
//   "users": { "email@example.com": { "cookie": "csrftoken=..; sessionid=..", "updated": "ISO" }, ... },
//   "lastUsed": "email@example.com"
// }
async function readSessionFile(file) {
    try {
        const txt = await fs.promises.readFile(file, 'utf8');
        const data = JSON.parse(txt);
        if (data && data.users) {
            // Already new format (or compatible)
            return data;
        }
        // Backward compatibility: old single-cookie format { cookie: "..." }
        if (data && typeof data.cookie === 'string') {
            return {
                users: { 'default': { cookie: data.cookie, updated: data.updated || new Date().toISOString() } },
                lastUsed: 'default'
            };
        }
    } catch { }
    return null;
}

async function writeSessionFileMulti(file, email, cookie, existing) {
    // email can be null -> store under 'default'
    const key = (email || 'default').trim().toLowerCase();
    let data = existing && existing.users ? existing : { users: {}, lastUsed: key };
    data.users[key] = { cookie, updated: new Date().toISOString() };
    data.lastUsed = key;
    try { await fs.promises.writeFile(file, JSON.stringify(data, null, 2), 'utf8'); } catch { }
}

async function fetchJson(url, referer) {
    const res = await fetchWithTimeout(url, { headers: { ...commonHeaders(referer), accept: 'application/json' } }, 30_000);
    const text = await res.text();
    let json = null; try { json = JSON.parse(text); } catch { }
    return { res, text, json };
}

function extractSetCookie(res) {
    // Node fetch in Node 18 does not expose raw set-cookie headers directly. We rely on manual cookie env or future enhancement.
    return null;
}

async function obtainCsrfToken() {
    const { json } = await fetchJson(`${ORIGIN}/api/v1/general/core-data/?profile=1`, ORIGIN);
    let csrf = json?.auth?.csrf;
    // Try to parse cookie from ACTIVE_COOKIE fallback
    if (!csrf) {
        // Not critical; some endpoints may still set it later.
    }
    return csrf;
}

import https from 'https';

// Manual minimal cookie store (in-memory) for login flow only
class SimpleCookieStore {
    constructor() { this.map = new Map(); }
    setCookieLine(line) {
        if (!line) return;
        const seg = line.split(';')[0];
        const eq = seg.indexOf('=');
        if (eq === -1) return;
        const k = seg.slice(0, eq).trim();
        const v = seg.slice(eq + 1).trim();
        if (k) this.map.set(k, v);
    }
    applySetCookie(arr) { (arr || []).forEach(l => this.setCookieLine(l)); }
    get(name) { return this.map.get(name); }
    headerString() { return Array.from(this.map.entries()).map(([k, v]) => `${k}=${v}`).join('; '); }
}

function rawRequest(urlStr, { method = 'GET', headers = {}, body = null } = {}) {
    const u = new URL(urlStr);
    return new Promise((resolve, reject) => {
        const opts = {
            method,
            hostname: u.hostname,
            path: u.pathname + (u.search || ''),
            protocol: u.protocol,
            headers
        };
        const req = https.request(opts, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                resolve({
                    status: res.statusCode || 0,
                    headers: res.headers,
                    body: Buffer.concat(chunks).toString('utf8')
                });
            });
        });
        req.on('error', reject);
        if (body) req.write(body);
        req.end();
    });
}

async function loginWithCredentialsInline(email, password, verbose = () => { }) {
    if (!email || !password) throw new Error('Email & password required for login');
    const store = new SimpleCookieStore();
    const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36';
    // Helper small debug printer (always go through verbose)
    const dbg = (...a) => verbose('[login]', ...a);

    // 0. Visit login page to obtain initial csrftoken cookie
    let r = await rawRequest(`${ORIGIN}/accounts/login/`, {
        method: 'GET',
        headers: {
            'User-Agent': UA,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
    });
    store.applySetCookie(r.headers['set-cookie']);
    let csrf = store.get('csrftoken') || null;
    if (!csrf) {
        // 0b. fallback: core-data json endpoint (sometimes returns csrf in body)
        const r2 = await rawRequest(`${ORIGIN}/api/v1/general/core-data/?profile=1`, {
            method: 'GET',
            headers: { 'User-Agent': UA, 'Accept': 'application/json' }
        });
        store.applySetCookie(r2.headers['set-cookie']);
        try { const j2 = JSON.parse(r2.body); csrf = csrf || j2?.auth?.csrf || null; } catch { }
        if (!csrf) csrf = store.get('csrftoken') || null;
        dbg('Fallback core-data for CSRF status:', r2.status);
    }
    if (!csrf) throw new Error('Cannot obtain CSRF token');
    dbg('CSRF token:', csrf.slice(0, 8) + '...');

    const cookieHeader = () => store.headerString();
    const baseHeaders = () => ({
        'User-Agent': UA,
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest'
    });
    const addCsrfHeaders = (h = {}) => ({
        ...h,
        'X-CSRFToken': csrf,
        'Origin': ORIGIN,
        'Referer': `${ORIGIN}/accounts/login/`
    });

    // 1. check-active-user
    const formCheck = new URLSearchParams();
    formCheck.append('csrfmiddlewaretoken', csrf);
    formCheck.append('tessera', email);
    // recaptcha sometimes optional; keep param but empty to mimic browser before token set
    formCheck.append('g-recaptcha-response', '');
    r = await rawRequest(`${ORIGIN}/api/v1/auth/check-active-user`, {
        method: 'POST',
        headers: addCsrfHeaders({
            ...baseHeaders(),
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': cookieHeader()
        }),
        body: formCheck.toString()
    });
    store.applySetCookie(r.headers['set-cookie']);
    let jCheck = null; try { jCheck = JSON.parse(r.body); } catch { }
    if (!jCheck) {
        dbg('check-active-user raw body:', r.body.slice(0, 300));
        throw new Error('check-active-user invalid JSON status=' + r.status);
    }
    dbg('check-active-user response:', jCheck.status, jCheck.message);
    if (jCheck.status !== 'success') {
        // Provide clearer error details
        throw new Error('check-active-user failed status=' + jCheck.status + ' message=' + jCheck.message);
    }
    if (jCheck.message !== 'get-pass') {
        throw new Error('Unsupported flow (expected get-pass, got ' + jCheck.message + ')');
    }
    dbg('check-active-user OK');

    // 2. login-authentication
    const formLogin = new URLSearchParams();
    formLogin.append('csrfmiddlewaretoken', csrf);
    formLogin.append('tessera', email);
    formLogin.append('hidden_username', email);
    formLogin.append('password', password);
    formLogin.append('g-recaptcha-response', '');
    r = await rawRequest(`${ORIGIN}/api/v1/auth/login-authentication`, {
        method: 'POST',
        headers: addCsrfHeaders({
            ...baseHeaders(),
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': cookieHeader()
        }),
        body: formLogin.toString()
    });
    store.applySetCookie(r.headers['set-cookie']);
    let jLogin = null; try { jLogin = JSON.parse(r.body); } catch { }
    if (!jLogin) {
        dbg('login-authentication raw body:', r.body.slice(0, 300));
        throw new Error('login-authentication invalid JSON status=' + r.status);
    }
    dbg('login-authentication response:', jLogin.status, jLogin.message);
    if (jLogin.status !== 'success') throw new Error('login-authentication failed message=' + jLogin.message);
    dbg('login-authentication OK');

    // Compose final cookie header (only what we need for reuse)
    const sessionid = store.get('sessionid');
    const csrftoken = store.get('csrftoken') || csrf;
    if (!sessionid) throw new Error('Session cookie missing after login');
    ACTIVE_COOKIE = `csrftoken=${csrftoken}; sessionid=${sessionid}`;
    dbg('ACTIVE_COOKIE prepared');
    return true;
}

async function prepareSession({ userEmail, userPassword, sessionFile, verbose, courseUrl, forceLogin }) {
    // Helper to verify current ACTIVE_COOKIE by calling core-data
    const verify = async () => {
        try {
            if (!ACTIVE_COOKIE) return null;
            verbose('Verifying existing session cookie...');
            const core = await fetchCoreData(courseUrl || ORIGIN);
            const ok = !!core?.auth?.details?.is_authenticated;
            if (ok) {
                logInfo('Session valid' + (userEmail ? ` (user: ${userEmail})` : ''));
                return core;
            }
            logWarn('Stored session not authenticated');
            return null;
        } catch (e) {
            verbose('Verify failed: ' + e.message);
            return null;
        }
    };

    // 1. Environment / explicit cookie overrides everything
    if (COOKIE && COOKIE !== 'PUT_YOUR_COOKIE_HERE') {
        ACTIVE_COOKIE = COOKIE;
        verbose('Using cookie from env / file override');
        const core = await verify();
        if (core) return { core, source: 'env' };
        // If env cookie invalid and we have credentials we can attempt login below.
    }

    // 2. Load multi-user session store if exists
    let sessionData = null;
    if (sessionFile) {
        sessionData = await readSessionFile(sessionFile);
    }

    const desiredUserKey = userEmail ? userEmail.trim().toLowerCase() : null;

    // 2a. If user specified, try existing cookie first (even if password provided) unless forceLogin
    if (sessionData && desiredUserKey && !forceLogin) {
        const entry = sessionData.users[desiredUserKey];
        if (entry && entry.cookie) {
            ACTIVE_COOKIE = entry.cookie;
            logStep(`Loaded stored session for user ${desiredUserKey}`);
            const core = await verify();
            if (core) {
                if (userPassword) verbose('Reusing valid stored session; skipping login because --force-login not set');
                return { core, source: 'stored-user' };
            }
            logWarn('Stored session invalid; will attempt fresh login if password provided.');
            ACTIVE_COOKIE = null; // clear invalid
        }
    }
    // 2b. If no user specified, try lastUsed
    if (sessionData && !desiredUserKey) {
        const key = sessionData.lastUsed;
        if (key && sessionData.users[key] && sessionData.users[key].cookie) {
            ACTIVE_COOKIE = sessionData.users[key].cookie;
            logStep(`Loaded lastUsed session (${key})`);
            const core = await verify();
            if (core) return { core, source: 'stored-last' };
            logWarn('Last used session invalid.');
        }
    }

    // 3. Need to login only if we have credentials AND either no session or it was invalid
    if (desiredUserKey && userPassword && (!ACTIVE_COOKIE || forceLogin)) {
        try {
            logStep('Attempting login for ' + desiredUserKey);
            await loginWithCredentialsInline(userEmail, userPassword, verbose);
            if (ACTIVE_COOKIE && sessionFile) {
                await writeSessionFileMulti(sessionFile, userEmail, ACTIVE_COOKIE, sessionData);
                logSuccess('Login success; session stored for user ' + desiredUserKey);
            }
            const core = await verify();
            if (core) return { core, source: 'fresh-login' };
        } catch (e) {
            logWarn('Inline login failed: ' + e.message);
        }
    }

    // 4. If we reach here, maybe we still have ACTIVE_COOKIE but verification failed or no cookie
    if (!ACTIVE_COOKIE) {
        logWarn('No usable session found. Provide --user and --pass to create one.');
    }
    return { core: null, source: 'none' };
}

// Extract <track ... src="..."> subtitle URLs from lecture HTML.
function extractSubtitleLinks(html) {
    const results = new Set();
    if (!html) return [];
    const re = /<track\b[^>]*?src=["']([^"'>]+)["'][^>]*>/gim;
    let m;
    while ((m = re.exec(html)) !== null) {
        const raw = m[1];
        const url = decodeHtmlEntities(raw);
        if (url) results.add(url);
    }
    return Array.from(results);
}

// Transform stream to limit to first N bytes and optionally signal upstream.
class ByteLimit extends Transform {
    // Limits the stream to the first `limit` bytes, then signals upstream to stop.
    constructor(limit, onLimit) { super(); this.limit = limit; this.seen = 0; this._hit = false; this._onLimit = onLimit; }
    _transform(chunk, enc, cb) {
        if (this.limit <= 0) { this.push(chunk); return cb(); }
        const remaining = this.limit - this.seen;
        if (remaining <= 0) { return cb(); }
        const buf = chunk.length > remaining ? chunk.subarray(0, remaining) : chunk;
        this.push(buf);
        this.seen += buf.length;
        if (!this._hit && this.seen >= this.limit) {
            this.end();
            this._hit = true;
            if (typeof this._onLimit === 'function') {
                try { this._onLimit(); } catch { }
            }
        }
        cb();
    }
}

// Download a URL to a file (with retries). If sampleBytes > 0, request a Range and also enforce a local limit.
// label: optional display name to show in the progress line (e.g., final file name)
// opts.liveProgress: when false (used for --parallel > 1), skips the live \r-updating bar to avoid interleaved output.
async function downloadToFile(url, filePath, referer, maxRetries = 3, sampleBytes = 0, label = '', opts = {}) {
    const liveProgress = opts.liveProgress !== false;
    let retries429Total = 0;
    // Skip if already exists with non-zero size
    let existingFinalSize = 0;
    try { const stat = fs.statSync(filePath); existingFinalSize = stat.size; if (existingFinalSize > 0 && sampleBytes > 0) return 'exists'; } catch { }
    const tmpPath = filePath + '.part';
    let existingTmpSize = 0;
    try { const stat = fs.statSync(tmpPath); existingTmpSize = stat.size; } catch { }

    // For full downloads, see if final is already complete
    let remoteInfo;
    if (sampleBytes === 0 && existingFinalSize > 0) {
        remoteInfo = await getRemoteSizeAndRanges(url, referer);
        if (remoteInfo.size && existingFinalSize >= remoteInfo.size) {
            return 'exists';
        }
    }

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            // Decide resume offset
            let resumeOffset = 0;
            let writingTo = tmpPath;
            if (sampleBytes > 0) {
                resumeOffset = 0; // do not resume sample downloads
            } else {
                if (existingTmpSize > 0) {
                    resumeOffset = existingTmpSize;
                } else if (existingFinalSize > 0) {
                    // Only resume from final if server supports ranges
                    if (!remoteInfo) remoteInfo = await getRemoteSizeAndRanges(url, referer);
                    if (remoteInfo.acceptRanges) {
                        // Move final to tmp to resume appending
                        try { await fs.promises.rename(filePath, tmpPath); existingTmpSize = existingFinalSize; resumeOffset = existingFinalSize; existingFinalSize = 0; } catch { }
                    } else {
                        // Cannot resume; start from scratch
                        resumeOffset = 0;
                    }
                }
            }

            const requestInit = { method: 'GET', headers: { ...commonHeaders(referer), accept: 'video/mp4,application/octet-stream,*/*' } };
            if (sampleBytes && sampleBytes > 0) {
                requestInit.headers['range'] = `bytes=0-${Math.max(0, sampleBytes - 1)}`;
            } else if (resumeOffset > 0) {
                requestInit.headers['range'] = `bytes=${resumeOffset}-`;
            }

            const controller = new AbortController();
            const to = setTimeout(() => controller.abort(), 120_000);
            let res = await fetch(url, { ...requestInit, signal: controller.signal });
            // Honor 429 Too Many Requests with Retry-After before treating it as a hard failure.
            while (res.status === 429 && retries429Total < 5) {
                retries429Total++;
                const ra = res.headers.get('retry-after');
                let waitMs = null;
                if (ra) {
                    const asSeconds = Number(ra);
                    waitMs = !isNaN(asSeconds) ? asSeconds * 1000 : Math.max(0, Date.parse(ra) - Date.now());
                }
                if (!waitMs || isNaN(waitMs)) waitMs = Math.min(30_000, 2000 * retries429Total);
                try { if (res.body) { const rb = Readable.fromWeb(res.body); rb.resume(); } } catch { }
                logWarn(`⏳ HTTP 429 on download — waiting ${Math.round(waitMs / 1000)}s (${retries429Total}/5) — ${label || path.basename(filePath)}`);
                await sleep(waitMs);
                res = await fetch(url, { ...requestInit, signal: controller.signal });
            }
            if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
            if (resumeOffset > 0 && res.status !== 206) {
                // Server didn't honor Range; restart from 0
                try { await fs.promises.unlink(tmpPath); } catch { }
                existingTmpSize = 0; resumeOffset = 0;
                clearTimeout(to);
                throw new Error('Server did not honor range; restarting from 0');
            }

            await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
            const write = fs.createWriteStream(writingTo, { flags: (sampleBytes > 0 || resumeOffset === 0) ? 'w' : 'a' });
            const readable = Readable.fromWeb(res.body);

            // Progress bar state
            const contentLengthHeader = res.headers.get('content-length');
            const fullLength = contentLengthHeader ? parseInt(contentLengthHeader, 10) : undefined;
            // Try content-range for total size when resuming
            let expectedTotal;
            const contentRange = res.headers.get('content-range');
            const crMatch = contentRange && contentRange.match(/\/(\d+)$/);
            if (sampleBytes && sampleBytes > 0) expectedTotal = sampleBytes;
            else if (crMatch) expectedTotal = parseInt(crMatch[1], 10);
            else if (fullLength && resumeOffset > 0) expectedTotal = resumeOffset + fullLength;
            else expectedTotal = fullLength;
            let downloadedBytes = resumeOffset;
            const startedAt = Date.now();

            // Progress render helper
            const truncate = (s, max = 70) => {
                if (!s) return '';
                const str = String(s);
                return str.length > max ? str.slice(0, max - 1) + '…' : str;
            };
            const render = (final = false) => {
                if (!liveProgress && !final) return; // suppress interleaved live bars when running in parallel
                const elapsedSec = Math.max(0.001, (Date.now() - startedAt) / 1000);
                const speed = downloadedBytes / elapsedSec;
                // clamp bytes to expected total when finalizing or very close (to avoid 99.9% stuck)
                let shownDownloaded = downloadedBytes;
                if (expectedTotal && (final || downloadedBytes > expectedTotal)) {
                    // Tolerate tiny overflow due to headers/rounding
                    const overflow = downloadedBytes - expectedTotal;
                    if (overflow <= 65536) shownDownloaded = expectedTotal;
                }
                // Decide ratio; if final, force full bar
                let ratio = 0;
                if (final) {
                    ratio = 1;
                } else if (expectedTotal) {
                    ratio = (shownDownloaded / expectedTotal);
                } else {
                    ratio = 0; // unknown total
                }
                const bar = buildProgressBar(ratio);
                const pct = final ? '100.0%' : (expectedTotal ? `${(Math.min(1, ratio) * 100).toFixed(1)}%` : '--%');
                const sizeStr = `${formatBytes(shownDownloaded)}${expectedTotal ? ' / ' + formatBytes(expectedTotal) : ''}`;
                const name = label ? `  -  ${truncate(label, 80)}` : '';
                if (final && !liveProgress) {
                    console.log(`  ⬇️  Done  ${sizeStr}  ${formatSpeed(speed)}${name}`);
                } else {
                    process.stdout.write(`\r${line(bar, pct, sizeStr, speed, name)}`);
                }
            };
            const line = (bar, pct, sizeStr, speed, name) => `  ⬇️  [${bar}] ${pct}  ${sizeStr}  ${formatSpeed(speed)}${name}`;

            // Counting transform
            const counter = new Transform({
                transform(chunk, _enc, cb) {
                    downloadedBytes += chunk.length;
                    // throttle render slightly by size steps
                    if (downloadedBytes === chunk.length || downloadedBytes % 65536 < 8192) render();
                    cb(null, chunk);
                }
            });
            let byteLimitReached = false;
            try {
                if (sampleBytes && sampleBytes > 0) {
                    const limiter = new ByteLimit(sampleBytes, () => {
                        byteLimitReached = true;
                        try { readable.destroy(new Error('byte-limit')); } catch { }
                        try { controller.abort(); } catch { }
                    });
                    await pipeline(readable, counter, limiter, write);
                } else {
                    await pipeline(readable, counter, write);
                }
            } catch (pipeErr) {
                if (sampleBytes && byteLimitReached) {
                    try { clearTimeout(to); } catch { }
                    try { render(true); process.stdout.write('\n'); } catch { }
                    try {
                        await fs.promises.rename(tmpPath, filePath);
                    } catch (e) {
                        try { await fs.promises.copyFile(writingTo, filePath); } catch { }
                    }
                    try { await fs.promises.unlink(tmpPath); } catch { }
                    return 'downloaded';
                }
                throw pipeErr;
            } finally {
                clearTimeout(to);
            }

            // finalize progress bar to 100%
            try { render(true); } catch { }
            process.stdout.write('\n');
            try {
                await fs.promises.rename(tmpPath, filePath);
            } catch (e) {
                try { await fs.promises.copyFile(writingTo, filePath); } catch { }
            }
            try { await fs.promises.unlink(tmpPath); } catch { }
            return 'downloaded';
        } catch (err) {
            try { process.stdout.write('\n'); } catch { }
            // Keep .part file for future resume; do not delete on error
            if (attempt < maxRetries) {
                logWarn(`Retry ${attempt}/${maxRetries} for ${path.basename(filePath)} after error: ${err.message}`);
                await sleep(1000 * attempt);
                continue;
            }
            throw err;
        }
    }
}

// ===============
// Shared per-unit video resolution (used by real download, --dry-run, and --estimate-size)
// ===============
// Resolves the best video URL (respecting `quality`) for a unit, trying the new LMS API first
// (unless this course was already confirmed to be on the old system), falling back to HTML scrape.
async function resolveUnitVideo(unit, lectureUrl, quality, referer) {
    if (courseApiMode !== 'old') {
        const attempt = await tryNewApiForUnit(unit, lectureUrl, quality);
        if (attempt.ok) {
            courseApiMode = 'new';
            return { ok: true, mode: 'new', url: attempt.videoUrl, unitDetail: attempt.unitDetail, downgraded: attempt.downgraded, qualityLabel: attempt.qualityLabel };
        }
        if (attempt.reason === 'locked') return { ok: false, reason: 'locked' };
        if (attempt.reason === 'not-found' && courseApiMode === null) courseApiMode = 'old';
        // any other failure falls through to try the old method below
    }
    try {
        const res = await fetchWithRetry429(lectureUrl, { headers: { ...commonHeaders(referer), accept: 'text/html' } });
        if (!res.ok) return { ok: false, reason: 'error', message: `HTTP ${res.status}` };
        const html = await res.text();
        const videoSources = extractVideoSources(html);
        const picked = pickSourceForQuality(videoSources, quality);
        if (!picked.url) return { ok: false, reason: 'no-video' };
        return { ok: true, mode: 'old', url: picked.url, html, downgraded: picked.downgraded, qualityLabel: picked.label };
    } catch (e) {
        return { ok: false, reason: 'error', message: e.message };
    }
}

// ===============
// Concurrency helper for --parallel
// ===============
async function runWithConcurrency(items, limit, worker) {
    let idx = 0;
    async function runner() {
        while (idx < items.length) {
            const current = idx++;
            await worker(items[current], current);
        }
    }
    const workerCount = Math.max(1, Math.min(limit, items.length || 1));
    await Promise.all(Array.from({ length: workerCount }, () => runner()));
}

// ===============
// CSV / HTML report helpers
// ===============
function csvEscape(v) {
    if (v == null) return '';
    const s = String(v);
    if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
}

function buildCsv(records, columns) {
    const header = columns.map(c => csvEscape(c.label)).join(',');
    const rows = records.map(r => columns.map(c => csvEscape(c.get(r))).join(','));
    return [header, ...rows].join('\r\n');
}

function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

const REPORT_COLUMNS = [
    { label: 'Chapter', get: r => r.chapterTitle },
    { label: 'Unit', get: r => r.unitTitle },
    { label: 'UnitId', get: r => r.unitId },
    { label: 'FileName', get: r => r.fileName },
    { label: 'Status', get: r => r.status },
    { label: 'Mode', get: r => r.mode || '' },
    { label: 'Reason', get: r => r.reason || '' },
    { label: 'FilePath', get: r => r.filePath || '' },
    { label: 'DownloadURL', get: r => r.videoUrl || '' },
];

// Approximate validity window observed on maktabkhooneh's tokenized CDN links (video/attachment URLs).
// This is empirical, not guaranteed by the server, so the wording stays conservative ("معمولاً").
const LINK_VALIDITY_NOTE_FA = 'این لینک‌ها توکن‌دار و کوتاه‌مدت هستند و معمولاً حدود ۱۵ تا ۳۰ دقیقه پس از تولید این گزارش منقضی می‌شوند؛ اگر مدتی بعد از این گزارش استفاده کنید، احتمالاً با خطای دسترسی مواجه خواهید شد.';

function writeReportFiles(outputRootFolder, records) {
    const generatedAt = new Date();
    const generatedAtIso = generatedAt.toISOString();
    const generatedAtFa = generatedAt.toLocaleString('fa-IR');

    const csv = buildCsv(records, REPORT_COLUMNS);
    const csvComment = `# Generated: ${generatedAtIso} — DownloadURL column: ${LINK_VALIDITY_NOTE_FA}`;
    const csvPath = path.join(outputRootFolder, 'report.csv');
    fs.writeFileSync(csvPath, '\uFEFF' + csvComment + '\r\n' + csv, 'utf8'); // BOM helps Excel render Persian/UTF-8 correctly

    const rowsHtml = records.map(r => {
        const linkCell = r.videoUrl
            ? `<a href="${escapeHtml(r.videoUrl)}" target="_blank" rel="noopener">لینک دانلود</a>`
            : '-';
        return `<tr class="status-${escapeHtml(r.status)}"><td>${escapeHtml(r.chapterTitle)}</td><td>${escapeHtml(r.unitTitle)}</td><td>${escapeHtml(String(r.unitId))}</td><td>${escapeHtml(r.fileName)}</td><td>${escapeHtml(r.status)}</td><td>${escapeHtml(r.mode || '-')}</td><td>${escapeHtml(r.reason || '')}</td><td>${linkCell}</td></tr>`;
    }).join('\n');
    const summary = records.reduce((acc, r) => { acc[r.status] = (acc[r.status] || 0) + 1; return acc; }, {});
    const summaryHtml = Object.entries(summary).map(([k, v]) => `<span class="badge">${escapeHtml(k)}: ${v}</span>`).join(' ');
    const html = `<!DOCTYPE html>
<html lang="fa" dir="rtl"><head><meta charset="utf-8"><title>Maktabkhooneh Downloader Report</title>
<style>
body{font-family:Tahoma,Arial,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px}
h1{font-size:20px}
.meta{color:#94a3b8;font-size:13px;margin-bottom:8px}
.warn{background:#3f2d0f;border:1px solid #92640f;color:#fde68a;padding:10px 14px;border-radius:8px;font-size:13px;margin:12px 0}
table{border-collapse:collapse;width:100%;margin-top:16px;font-size:13px}
th,td{border:1px solid #334155;padding:6px 10px;text-align:right}
th{background:#1e293b}
tr.status-downloaded{background:#052e1a}
tr.status-failed{background:#3f0f0f}
tr.status-locked{background:#3f2d0f}
tr.status-skipped-exists{background:#1e293b}
tr.status-no-video{background:#3f0f0f}
.badge{display:inline-block;padding:4px 10px;border-radius:12px;background:#1e293b;margin-inline-end:8px}
a{color:#38bdf8}
</style></head>
<body>
<h1>📋 گزارش دانلود دوره</h1>
<div class="meta">🕒 زمان تولید گزارش: ${escapeHtml(generatedAtFa)} (${escapeHtml(generatedAtIso)})</div>
<div class="warn">⚠️ ستون «لینک دانلود» توکن‌دار است. ${escapeHtml(LINK_VALIDITY_NOTE_FA)}</div>
<div>${summaryHtml}</div>
<table><thead><tr><th>فصل</th><th>عنوان</th><th>Unit ID</th><th>فایل</th><th>وضعیت</th><th>حالت</th><th>توضیح</th><th>لینک دانلود</th></tr></thead>
<tbody>${rowsHtml}</tbody></table>
</body></html>`;
    fs.writeFileSync(path.join(outputRootFolder, 'report.html'), html, 'utf8');
    console.log(`📄 Reports written: ${paintCyan('report.csv')}, ${paintCyan('report.html')}`);
}

// Minimal CSV line parser (matches the quoting style produced by csvEscape above).
function parseCsvLine(line) {
    const result = [];
    let cur = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const c = line[i];
        if (inQuotes) {
            if (c === '"') {
                if (line[i + 1] === '"') { cur += '"'; i++; } else { inQuotes = false; }
            } else cur += c;
        } else {
            if (c === '"') inQuotes = true;
            else if (c === ',') { result.push(cur); cur = ''; }
            else cur += c;
        }
    }
    result.push(cur);
    return result;
}

// Load the set of unit IDs marked "failed" in the last report.csv for this course (for --retry-failed).
function loadFailedUnitIdsFromReport(outputRootFolder) {
    const csvPath = path.join(outputRootFolder, 'report.csv');
    if (!fs.existsSync(csvPath)) return null;
    let content;
    try { content = fs.readFileSync(csvPath, 'utf8'); } catch { return null; }
    if (content.charCodeAt(0) === 0xFEFF) content = content.slice(1);
    let lines = content.split(/\r\n|\n/).filter(Boolean);
    // Skip the leading "# Generated: ..." comment line written by writeReportFiles, if present.
    if (lines.length && lines[0].startsWith('#')) lines = lines.slice(1);
    if (lines.length < 2) return null;
    const header = parseCsvLine(lines[0]);
    const unitIdIdx = header.indexOf('UnitId');
    const statusIdx = header.indexOf('Status');
    if (unitIdIdx === -1 || statusIdx === -1) return null;
    const set = new Set();
    for (let i = 1; i < lines.length; i++) {
        const cols = parseCsvLine(lines[i]);
        if (cols[statusIdx] === 'failed' && cols[unitIdIdx]) set.add(String(cols[unitIdIdx]));
    }
    return set;
}

// ===============
// --estimate-size
// ===============
async function estimateCourseSize(tasks, referer, quality) {
    logStep(`📏 Estimating total size for ${tasks.length} unit(s)... (extra requests, may take a while)`);
    let totalBytes = 0, resolvedCount = 0, unknownCount = 0, lockedCount = 0;
    for (const task of tasks) {
        const resolved = await resolveUnitVideo(task.unit, task.lectureUrl, quality, referer);
        if (!resolved.ok) {
            if (resolved.reason === 'locked') lockedCount++; else unknownCount++;
            continue;
        }
        let info;
        try { info = await getRemoteSizeAndRanges(resolved.url, task.lectureUrl); } catch { info = {}; }
        if (info.size) { totalBytes += info.size; resolvedCount++; } else unknownCount++;
    }
    console.log(`📏 Estimated total size (${resolvedCount}/${tasks.length} resolved): ${paintBold(formatBytes(totalBytes))}` +
        (unknownCount ? paintYellow(`  (+${unknownCount} unknown)`) : '') +
        (lockedCount ? paintYellow(`  — ${lockedCount} locked`) : ''));
}

// ===============
// --dry-run
// ===============
async function runDryRun(tasks, referer, quality, outputRootFolder) {
    logStep(`🧪 Dry run: resolving ${tasks.length} unit(s) without downloading...`);
    const records = [];
    let availableCount = 0, lockedCount = 0, errorCount = 0;
    for (const task of tasks) {
        const resolved = await resolveUnitVideo(task.unit, task.lectureUrl, quality, referer);
        let status, note = '';
        if (resolved.ok) {
            status = 'available'; availableCount++;
            note = resolved.downgraded ? `quality fallback: ${resolved.qualityLabel}` : (resolved.qualityLabel || '');
        } else if (resolved.reason === 'locked') {
            status = 'locked'; lockedCount++;
        } else {
            status = 'error'; errorCount++;
            note = resolved.message || resolved.reason || '';
        }
        const icon = status === 'available' ? '✅' : status === 'locked' ? '🔒' : '❌';
        console.log(`${icon} ${task.finalFileName}${note ? ' — ' + note : ''}`);
        records.push({
            chapterTitle: task.chapter.title || task.chapter.slug, unitTitle: task.unit.title || task.unit.slug,
            unitId: task.unit.id, fileName: task.finalFileName, status, mode: resolved.mode || '', reason: note, filePath: '',
            videoUrl: resolved.ok ? resolved.url : ''
        });
    }
    const generatedAtIso = new Date().toISOString();
    const csv = buildCsv(records, REPORT_COLUMNS);
    const csvComment = `# Generated: ${generatedAtIso} — DownloadURL column: ${LINK_VALIDITY_NOTE_FA}`;
    const csvPath = path.join(outputRootFolder, 'dry-run-list.csv');
    fs.writeFileSync(csvPath, '\uFEFF' + csvComment + '\r\n' + csv, 'utf8');
    console.log('—'.repeat(40));
    console.log(`🧪 Dry run complete. Available: ${paintGreen(String(availableCount))}  Locked: ${paintYellow(String(lockedCount))}  Errors: ${paintRed(String(errorCount))}`);
    console.log(`📄 List written: ${paintCyan(csvPath)}`);
}

// ===============
// --verify-integrity
// ===============
async function verifyDownloadedIntegrity(records, referer) {
    const candidates = records.filter(r => (r.status === 'downloaded' || r.status === 'skipped-exists') && r.videoUrl && r.filePath);
    if (candidates.length === 0) { logInfo('No downloaded files to verify.'); return; }
    logStep(`🔍 Verifying integrity of ${candidates.length} file(s)...`);
    const suspects = [];
    for (const r of candidates) {
        let actualSize = 0;
        try { actualSize = fs.statSync(r.filePath).size; } catch { continue; }
        let info = {};
        try { info = await getRemoteSizeAndRanges(r.videoUrl, referer); } catch { }
        if (info.size && Math.abs(info.size - actualSize) > 65536) {
            suspects.push({ ...r, actualSize, expectedSize: info.size });
            logWarn(`⚠️ Size mismatch: ${r.fileName} — local ${formatBytes(actualSize)} vs server ${formatBytes(info.size)}`);
        }
    }
    if (suspects.length === 0) { logSuccess('All checked files match the expected size.'); return; }

    console.log(`\n${paintRed(String(suspects.length))} file(s) look incomplete/mismatched:`);
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    try {
        for (const s of suspects) {
            let answer = '';
            try { answer = (await rl.question(`Re-download "${s.fileName}"? (y/N) `)).trim().toLowerCase(); }
            catch { answer = ''; }
            if (answer === 'y' || answer === 'yes') {
                try { fs.unlinkSync(s.filePath); } catch { }
                try { fs.unlinkSync(s.filePath + '.part'); } catch { }
                console.log(`📥 Re-downloading: ${s.fileName}`);
                try {
                    const status = await downloadToFile(s.videoUrl, s.filePath, referer, 3, 0, '');
                    if (status !== 'exists') logSuccess(`RE-DOWNLOADED: ${s.fileName}`);
                } catch (e) {
                    logError(`Re-download failed for ${s.fileName}: ${e.message}`);
                }
            }
        }
    } finally {
        rl.close();
    }
}

// ===============
// Termux (Android) integrations — safe no-ops on any other platform / without termux-api installed
// ===============
import { execFile } from 'child_process';
import { promisify } from 'util';
const execFileAsync = promisify(execFile);

async function commandExists(cmd) {
    try { await execFileAsync('which', [cmd]); return true; } catch { return false; }
}

let wakeLockAcquired = false;
async function tryAcquireWakeLock() {
    if (!(await commandExists('termux-wake-lock'))) return false;
    try { await execFileAsync('termux-wake-lock'); wakeLockAcquired = true; logInfo('🔓 Termux wake-lock acquired (screen won\'t sleep during download).'); return true; }
    catch (e) { logWarn('Could not acquire termux-wake-lock: ' + e.message); return false; }
}
async function releaseWakeLockIfHeld() {
    if (!wakeLockAcquired) return;
    try { await execFileAsync('termux-wake-unlock'); logInfo('🔒 Termux wake-lock released.'); }
    catch { /* non-fatal */ }
    wakeLockAcquired = false;
}
async function tryNotify(title, content) {
    if (!(await commandExists('termux-notification'))) return false;
    try { await execFileAsync('termux-notification', ['--title', title, '--content', content]); return true; }
    catch { return false; }
}

async function main() {
    const {
        inputCourseUrl, sampleBytesToDownload, isVerboseLoggingEnabled, userEmail, userPassword, sessionFile, forceLogin,
        quality, parallel, chaptersSpec, noSubtitle, noAttachments, noLog, estimateSize, dryRun, retryFailed, verifyIntegrity,
        outputDir, noWakeLock, noNotify
    } = parseCLI();
    const { verbose } = createVerboseLogger(isVerboseLoggingEnabled);
    if (!inputCourseUrl) { printUsage(); process.exit(1); }
    // Attempt to load / create / verify session (may already return core)
    const prep = await prepareSession({ userEmail, userPassword, sessionFile, verbose, courseUrl: inputCourseUrl, forceLogin });
    ensureCookiePresent();

    const normalizedCourseUrl = ensureTrailingSlash(inputCourseUrl.trim());
    const courseSlug = extractCourseSlug(normalizedCourseUrl);
    // Use decoded slug (human-friendly, especially for Persian) for the top-level folder name
    const courseDisplayName = sanitizeName(decodeURIComponent(courseSlug));
    // --output lets the download root be redirected anywhere (e.g. ~/storage/downloads on Termux);
    // default stays exactly as before: ./download/<courseName>
    const outputBase = outputDir ? path.resolve(process.cwd(), outputDir) : path.resolve(process.cwd(), 'download');
    const outputRootFolder = path.join(outputBase, courseDisplayName);
    // Ensure base output folder exists
    try { await fs.promises.mkdir(outputRootFolder, { recursive: true }); } catch { }

    // Log file (enabled by default; disable with --no-log)
    if (!noLog) {
        const logPath = path.join(outputRootFolder, 'download.log');
        if (setupLogFile(logPath)) console.log(`🧾 Logging to: ${paintCyan(logPath)}`);
    }

    // Termux: keep the screen/CPU awake for the duration of the download (safe no-op elsewhere / if not installed)
    if (!noWakeLock) await tryAcquireWakeLock();

    // Verify auth profile (reuse from prepareSession if available)
    let coreData = prep.core;
    if (!coreData) {
        try {
            coreData = await fetchCoreData(normalizedCourseUrl);
        } catch (e) {
            logError('Failed to verify authentication:', e.message);
            process.exit(1);
        }
    }
    const ok = printProfileSummary(coreData);
    if (!ok) { logError('Not logged in. Session invalid. Provide credentials with --user --pass.'); process.exit(1); }

    console.log(`📚 Course slug: ${paintBold(decodeURIComponent(courseSlug))}`);
    console.log(`📁 Output folder: ${paintCyan(outputRootFolder)}`);
    if (sampleBytesToDownload && sampleBytesToDownload > 0) {
        console.log(`🎯 Sample mode: downloading first ${paintBold(String(sampleBytesToDownload))} bytes of each video (saved as .sample.mp4)`);
    }
    if (quality && quality !== 'best') console.log(`🎚️  Preferred quality: ${paintBold(quality)}`);
    if (parallel > 1) console.log(`⚡ Parallel downloads: ${paintBold(String(parallel))}`);

    // Fetch chapters
    verbose(paintCyan('Fetching chapters...'));
    const chaptersData = await fetchChapters(courseSlug, normalizedCourseUrl);
    const chapters = Array.isArray(chaptersData?.chapters) ? chaptersData.chapters : [];
    if (chapters.length === 0) { logError('No chapters found. Make sure the URL and cookie are correct.'); process.exit(2); }

    const chapterFilterSet = parseChapterSpec(chaptersSpec);
    if (chapterFilterSet) console.log(`📌 Chapter filter: ${paintBold([...chapterFilterSet].sort((a, b) => a - b).join(','))}`);

    // --retry-failed: load previous report.csv (if any) and build the set of unit IDs to retry.
    let retryFailedUnitIds = null;
    if (retryFailed) {
        retryFailedUnitIds = loadFailedUnitIdsFromReport(outputRootFolder);
        if (!retryFailedUnitIds || retryFailedUnitIds.size === 0) {
            logWarn('No previous failed units found in report.csv for this course. Nothing to retry.');
            return;
        }
        console.log(`🔁 Retry-failed: ${paintBold(String(retryFailedUnitIds.size))} unit(s) from the last report`);
    }

    // ---- Build the flat task list (chapter × unit), applying filters ----
    const nonLectureCounts = {};
    const tasks = [];
    for (let chapterIndex = 0; chapterIndex < chapters.length; chapterIndex++) {
        const chapter = chapters[chapterIndex];
        const chapterNum = chapterIndex + 1;
        if (chapterFilterSet && !chapterFilterSet.has(chapterNum)) continue;
        const chapterOrder = String(chapterNum).padStart(2, '0');
        const chapterFolder = path.join(outputRootFolder, `${chapterOrder} - ${sanitizeName(chapter.title || chapter.slug || 'chapter')}`);
        // Support both old API (unit_set) and new API (units)
        const units = Array.isArray(chapter.units) ? chapter.units : (Array.isArray(chapter.unit_set) ? chapter.unit_set : []);
        for (let unitIndex = 0; unitIndex < units.length; unitIndex++) {
            const unit = units[unitIndex];
            // Old API: skip if status is explicitly falsy; new API has no status field so skip this check
            if ('status' in unit && !unit.status) continue; // inactive (old API)
            if (unit?.type !== 'lecture') {
                const t = unit?.type || 'unknown';
                nonLectureCounts[t] = (nonLectureCounts[t] || 0) + 1;
                continue; // skip non-video units (quiz, project, text, ...)
            }
            if (retryFailedUnitIds && !retryFailedUnitIds.has(String(unit.id))) continue;
            const unitOrder = String(unitIndex + 1).padStart(2, '0');
            const baseFileName = `${unitOrder} - ${sanitizeName(unit.title || unit.slug || 'lecture')}.mp4`;
            const finalFileName = (sampleBytesToDownload && sampleBytesToDownload > 0)
                ? baseFileName.replace(/\.mp4$/i, '.sample.mp4')
                : baseFileName;
            const outputFilePath = path.join(chapterFolder, finalFileName);
            const lectureUrl = buildLectureUrl(courseSlug, chapter, unit);
            tasks.push({ chapter, chapterNum, chapterFolder, unit, unitIndex, finalFileName, outputFilePath, lectureUrl });
        }
    }

    console.log(`🔎 Found ${paintBold(String(tasks.length))} lecture unit(s) to process` +
        (chapterFilterSet ? ' (after chapter filter)' : '') + (retryFailed ? ' (retry-failed mode)' : ''));
    if (Object.keys(nonLectureCounts).length > 0) {
        const parts = Object.entries(nonLectureCounts).map(([t, n]) => `${n} ${t}`).join(', ');
        logInfo(`Also found non-video content this tool doesn't download: ${parts}`);
    }
    if (tasks.length === 0) { logWarn('Nothing to do.'); return; }

    // ---- --estimate-size (opt-in, before real downloading) ----
    if (estimateSize) {
        await estimateCourseSize(tasks, normalizedCourseUrl, quality);
    }

    // ---- --dry-run: list only, write a CSV, and exit (no downloading) ----
    if (dryRun) {
        await runDryRun(tasks, normalizedCourseUrl, quality, outputRootFolder);
        return;
    }

    // ---- Real download ----
    let downloadedCount = 0, skippedCount = 0, failedCount = 0;
    const reportRecords = [];

    const processUnit = async (task) => {
        const { chapter, chapterFolder, unit, finalFileName, outputFilePath, lectureUrl } = task;
        const record = {
            chapterTitle: chapter.title || chapter.slug, unitTitle: unit.title || unit.slug, unitId: unit.id,
            fileName: finalFileName, status: null, reason: '', mode: null, filePath: outputFilePath, videoUrl: null
        };

        // Old-API static lock flag (new-API locked units are detected below from the actual response).
        if (unit.locked === true) {
            logWarn(`🔒 Locked/No access: ${finalFileName}`);
            skippedCount++;
            record.status = 'locked';
            reportRecords.push(record);
            return;
        }

        try {
            const resolved = await resolveUnitVideo(unit, lectureUrl, quality, normalizedCourseUrl);
            if (!resolved.ok) {
                if (resolved.reason === 'locked') {
                    logWarn(`🔒 Locked/No access: ${finalFileName}`);
                    skippedCount++;
                    record.status = 'locked';
                    reportRecords.push(record);
                    return;
                }
                logWarn(`No video source found for: ${finalFileName}${resolved.message ? ' — ' + resolved.message : ''}`);
                skippedCount++;
                record.status = 'no-video';
                record.reason = resolved.message || resolved.reason || '';
                reportRecords.push(record);
                return;
            }

            const { mode, url: bestVideoUrl, unitDetail, html, downgraded, qualityLabel } = resolved;
            record.mode = mode;
            record.videoUrl = bestVideoUrl;
            if (downgraded) logWarn(`Quality fallback (wanted "${quality}", got "${qualityLabel || 'lower'}"): ${finalFileName}`);

            console.log(`📥 Downloading: ${finalFileName}`);
            const status = await downloadToFile(bestVideoUrl, outputFilePath, lectureUrl, 3, sampleBytesToDownload, '', { liveProgress: parallel === 1 });
            if (status === 'exists') { console.log(paintYellow(`🟡 SKIP exists: ${finalFileName}`)); skippedCount++; record.status = 'skipped-exists'; }
            else { logSuccess(`DOWNLOADED: ${finalFileName}`); downloadedCount++; record.status = 'downloaded'; }

            const videoBaseNoExt = finalFileName.replace(/\.sample\.mp4$/i, '').replace(/\.mp4$/i, '');

            // Resolve subtitle/attachment source lists based on which mode we're in.
            let subtitleTargets = [];
            let attachmentTargets = [];
            if (mode === 'new') {
                if (unitDetail?.has_caption && unitDetail?.caption_file) subtitleTargets.push({ url: unitDetail.caption_file });
                attachmentTargets = extractNewAttachments(unitDetail);
            } else {
                subtitleTargets = extractSubtitleLinks(html).map(sUrl => ({
                    url: (() => { try { return new URL(sUrl, ORIGIN).toString(); } catch { return sUrl; } })()
                }));
                attachmentTargets = extractAttachmentLinks(html).map(attUrl => ({ url: attUrl, name: null }));
            }

            // ---- Subtitles (download beside video, same base name) ----
            if (!noSubtitle) {
                try {
                    for (const sub of subtitleTargets) {
                        try {
                            const absUrl = sub.url;
                            let ext = '.vtt';
                            try { const up = new URL(absUrl); ext = path.extname(up.pathname) || '.vtt'; } catch { }
                            const subtitleName = `${videoBaseNoExt}${ext}`;
                            const subtitlePath = path.join(chapterFolder, subtitleName);
                            if (fs.existsSync(subtitlePath) && fs.statSync(subtitlePath).size > 0) {
                                console.log(paintYellow(`🟡 Subtitle exists: ${subtitleName}`));
                                continue;
                            }
                            console.log(`📝 Subtitle: ${subtitleName}`);
                            const sStatus = await downloadToFile(absUrl, subtitlePath, lectureUrl, 3, 0, '', { liveProgress: parallel === 1 });
                            if (sStatus === 'exists') console.log(paintYellow(`🟡 Subtitle exists: ${subtitleName}`));
                            else logSuccess(`SUBTITLE: ${subtitleName}`);
                            await sleep(150);
                        } catch (subErr) { logWarn(`Subtitle fail: ${subErr.message}`); }
                    }
                } catch (subOuter) { logWarn(`Subtitle parse error: ${subOuter.message}`); }
            }

            // ---- Attachments (download beside video) ----
            if (!noAttachments) {
                try {
                    for (const att of attachmentTargets) {
                        try {
                            const attUrl = att.url;
                            let filePart = att.name;
                            if (!filePart) {
                                try {
                                    const u = new URL(attUrl);
                                    filePart = u.pathname.split('/').pop() || 'attachment.bin';
                                } catch { filePart = attUrl.split('?')[0].split('/').pop() || 'attachment.bin'; }
                            }
                            const sanitizedAttachment = sanitizeName(filePart);
                            const finalAttachmentName = `${videoBaseNoExt} - ${sanitizedAttachment}`;
                            const attachmentPath = path.join(chapterFolder, finalAttachmentName);
                            if (fs.existsSync(attachmentPath) && fs.statSync(attachmentPath).size > 0) {
                                console.log(paintYellow(`🟡 Attachment exists: ${finalAttachmentName}`));
                                continue;
                            }
                            console.log(`📎 Attachment: ${finalAttachmentName}`);
                            const aStatus = await downloadToFile(attUrl, attachmentPath, lectureUrl, 3, 0, '', { liveProgress: parallel === 1 });
                            if (aStatus === 'exists') console.log(paintYellow(`🟡 Attachment exists: ${finalAttachmentName}`));
                            else logSuccess(`ATTACHMENT: ${finalAttachmentName}`);
                            await sleep(200);
                        } catch (attErr) {
                            logWarn(`Attachment fail: ${attErr.message}`);
                        }
                    }
                } catch (attOuterErr) {
                    logWarn(`Attachment parse error: ${attOuterErr.message}`);
                }
            }

            // polite pause (shorter when running multiple downloads concurrently)
            await sleep(parallel === 1 ? 400 : 100);
        } catch (err) {
            logError(`FAIL ${finalFileName}: ${err.message}`);
            failedCount++;
            record.status = 'failed';
            record.reason = err.message;
        }
        reportRecords.push(record);
    };

    try {
        if (parallel <= 1) {
            for (const task of tasks) await processUnit(task);
        } else {
            await runWithConcurrency(tasks, parallel, processUnit);
        }
    } finally {
        console.log('—'.repeat(40));
        console.log(`📊 Total lecture units: ${paintBold(String(tasks.length))}`);
        console.log(`✅ Downloaded: ${paintGreen(String(downloadedCount))}`);
        console.log(`🟡 Skipped: ${paintYellow(String(skippedCount))}`);
        console.log(`❌ Failed: ${paintRed(String(failedCount))}`);
        if (Object.keys(nonLectureCounts).length > 0) {
            const parts = Object.entries(nonLectureCounts).map(([t, n]) => `${n} ${t}`).join(', ');
            console.log(`ℹ️  Non-video content skipped (not supported by this tool): ${parts}`);
        }

        try { writeReportFiles(outputRootFolder, reportRecords); } catch (e) { logWarn('Could not write report files: ' + e.message); }

        if (verifyIntegrity) {
            try { await verifyDownloadedIntegrity(reportRecords, normalizedCourseUrl); }
            catch (e) { logWarn('Integrity verification failed: ' + e.message); }
        }

        if (!noNotify) {
            const summary = `✅ ${downloadedCount} downloaded, 🟡 ${skippedCount} skipped, ❌ ${failedCount} failed`;
            await tryNotify(`Maktabkhooneh: ${courseDisplayName}`, summary);
        }
        if (!noWakeLock) await releaseWakeLockIfHeld();
    }
}

main().catch(async err => {
    logError('Fatal:', err);
    try { await releaseWakeLockIfHeld(); } catch { }
    process.exit(1);
});
