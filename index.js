// ERFAN-MD - index.js
// ESM Main Entry
// Auto Restart
// MongoDB Session & Settings
// External Plugins
// Newsletter Manager
// AntiLink Fix
// Connected Spam Fix
// Follow Repo Support
// AntiDelete/AntiEdit use lib/store.js (per-session, 200-msg limit, 5-min auto-clear)
//
// -- FILE MAP (sirf reference ke liye) --
//   1. IMPORTS
//   2. EXTERNAL LOADER CONFIG (repo URLs)
//   3. GLOBAL STATE / CONSTANTS (sessions, locks, guards, emoji lists)
//   4. LOGGER
//   5. EXTERNAL PLUGIN LOADER            -> loadExternalPlugins()
//   6. NEWSLETTER MANAGER LOADER         -> loadNewslettersManager()
//   7. FOLLOW REPO LOADER                -> loadFollowRepo()
//   8. REACTION REPO LOADER              -> loadReactionRepo()
//   9. CONNECTION STATUS HELPERS         -> isConnected(), connectionStatus()
//  10. RECONNECT-TOO-FAST GUARD          -> isReconnectingTooFast()
//  11. NEWSLETTER FOLLOW HANDLER         -> handleNewsletters()
//  12. NEWSLETTER AUTO-REACT HANDLER     -> setupNewsletterReactions()
//  13. MAIN SESSION START (WhatsApp socket + saare event listeners)
//                                        -> startSession()
//        13a. connection.update (open/close + reconnect chain)
//        13b. call (anti-call)
//        13c. group-participants.update (group events)
//        13d. messages.upsert (saara command/feature handling)
//  14. AUTO RECONNECT ALL               -> autoReconnectAll()
//  15. EXPRESS APP + ROUTES             (/, /code, /status, /disconnect, ...)
//  16. PROCESS SIGNAL HANDLERS          (SIGINT, SIGTERM, uncaughtException)
//  17. MAIN BOOT FUNCTION               -> main()
// --------------------------------------------------------

// -- 1. IMPORTS --
import express from 'express';
import fs from 'fs-extra';
import fsSync from 'fs';
import path from 'path';
import pino from 'pino';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

import {
    default as makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    delay,
    getContentType,
    makeCacheableSignalKeyStore,
    jidNormalizedUser,
    fetchLatestBaileysVersion,
    Browsers,
} from '@whiskeysockets/baileys';

import config from './config.js';
import { commands, cmd } from './command.js';
import { sms } from './lib/handler.js';
import { AntiDelete } from './lib/antidel.js';
import AntiEdit from './lib/antiedit.js';
import GroupEvents from './lib/groupevents.js';
import { addConnectionFunctions } from './lib/connection.js';
import { getGroupAdmins, lidToPhone } from './lib/functions.js';
import { saveMessage } from './lib/store.js';
import {
    connectMongo,
    saveSession,
    getSession,
    deleteSession,
    addNumber,
    removeNumber,
    getAllNumbers,
    getUserConfig,
    updateUserConfig,
    deleteUserConfig,
} from './lib/mongo.js';

// -- 2. EXTERNAL LOADER CONFIG --
const PLUGINS_REPO = 'https://raw.githubusercontent.com/ai-290/ai/main/plugins';
const FOLLOW_REPO_RAW_URL = 'https://raw.githubusercontent.com/ai-290/ai/main/lib/newsletters.js';
// Reaction repo — same pattern as FOLLOW_REPO_RAW_URL, but for the
// channel-react JID list (lib/reaction.js in the same external repo).
const REACTION_REPO_RAW_URL = 'https://raw.githubusercontent.com/ai-290/ai/main/lib/reaction.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// -- 3. GLOBAL STATE / CONSTANTS --
const sessions = new Map();
const sessionStartedAt = new Map();
const locks = new Map();
const MAX_ACTIVE = 50;

// Connected Message Guard
const connectMsgSentFor = new Set();

// Reconnect Control
const reconnectAttempts = new Map();
const RECONNECT_WINDOW_MS = 60000;
const RECONNECT_MAX_IN_WINDOW = 5;

// Group Sync Guard
const sessionReadyAt = new Map();
const GROUP_SYNC_GRACE_MS = 20000;
const GROUP_SYNC_BULK_THRESHOLD = 3;

// WhatsApp Link Regex
const LINK_REGEX = /(chat\.whatsapp\.com\/\S+)|(whatsapp\.com\/channel\/\S+)/i;

// Newsletter Auto-React Config
// This list is now populated from the external reaction repo (see
// loadReactionRepo() below) — same mechanism as the follow list. It's
// declared with `let` (not `const`) because loadReactionRepo() merges
// the fetched JIDs into it at boot. It starts empty and is filled in
// before any sessions connect.
let NEWSLETTER_REACT_JIDS = [];

const NEWSLETTER_REACT_EMOJIS = [
    "❤️", "👍", "😮", "😎", "😘", "🔥", "✨", "💖", "🤍", "🥀",
    "💫", "🌸", "⚡", "🤝", "🎉", "🥺", "😍", "😈", "🤖", "👀",
    "💯", "🎶", "🖤", "💥", "🌟", "😴", "🫶", "🍂", "☠️", "🌈",
    "🦋", "💎", "🎧", "📸", "🚀", "😏", "🤩", "🌹", "🎭", "🕊️",
    "🐼", "🐣", "🌙", "☁️", "🍁", "🎀", "🧸", "🍓", "🍒", "🌼",
    "🎯", "🏆", "🪐", "🌊", "🐉", "😜", "💌", "📍", "🎵", "🕶️",
    "🪄", "💋", "🌺", "🍀",
];

// -- 4. LOGGER --
function log(msg, type = 'info') {
    const icons = { info: '📝', success: '✅', error: '❌', warning: '⚠️', debug: '🐛' };
    console.log(`${icons[type] || '📝'} [ERFAN-MD] ${new Date().toISOString()}: ${msg}`);
}

// -- 5. EXTERNAL PLUGIN LOADER --
// Fetch Raw File
async function fetchRawText(url) {
    try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.text();
    } catch (e) {
        log(`Failed to fetch ${url}: ${e.message}`, 'error');
        return null;
    }
}

// Load External Plugins
async function loadExternalPlugins() {
    log('Loading external plugins from GitHub...');

    // === OLD FOLDER DELETE FIX ===
    // Pehle purana .temp_plugins folder delete karo taake purane plugins mix na hon
    const tempPluginsDir = path.join(__dirname, '.temp_plugins');
    try {
        if (fsSync.existsSync(tempPluginsDir)) {
            await fs.remove(tempPluginsDir);
            log('Old .temp_plugins folder deleted successfully', 'success');
        }
    } catch (e) {
        log(`Failed to delete old plugins folder: ${e.message}`, 'error');
    }
    // ==============================

    const apiUrl = 'https://api.github.com/repos/ai-290/ai/contents/plugins';
    let pluginFiles = [];

    try {
        const res = await fetch(apiUrl);
        if (res.ok) {
            const data = await res.json();
            pluginFiles = data.filter(f => f.name.endsWith('.js')).map(f => f.name);
        }
    } catch (e) {
        log(`GitHub API failed: ${e.message}`, 'warning');
        pluginFiles = [];
    }

    if (pluginFiles.length === 0) {
        log('No plugins found via API. Please ensure repo is public.', 'warning');
        return;
    }

    log(`Found ${pluginFiles.length} external plugins...`);

    for (const file of pluginFiles) {
        const rawUrl = `${PLUGINS_REPO}/${file}`;
        try {
            const code = await fetchRawText(rawUrl);
            if (!code) continue;

            const tempPath = path.join(__dirname, '.temp_plugins', file);
            await fs.ensureDir(path.dirname(tempPath));
            await fs.writeFile(tempPath, code);

            await import(tempPath + `?update=${Date.now()}`);
            log(`Loaded external plugin: ${file}`, 'success');
        } catch (e) {
            log(`Failed to load external plugin ${file}: ${e.message}`, 'error');
        }
    }

    log(`Total commands loaded: ${commands.length}`, 'success');
}

// -- 6. NEWSLETTER MANAGER LOADER --
let newsletterManager = null;

async function loadNewslettersManager() {
    log('Loading newsletters manager...');
    try {
        const mod = await import('./lib/newsletters.js?update=' + Date.now());
        newsletterManager = mod.default || mod;
        log('Newsletters manager loaded successfully', 'success');
    } catch (e) {
        log(`Failed to load newsletters: ${e.message}`, 'error');
    }
}

// -- 7. FOLLOW REPO LOADER --
async function loadFollowRepo() {
    log('Loading external follow-list repo...');
    try {
        const code = await fetchRawText(FOLLOW_REPO_RAW_URL);
        if (!code) return;

        const tempPath = path.join(__dirname, '.temp_plugins', `follow_repo_${Date.now()}.js`);
        await fs.ensureDir(path.dirname(tempPath));
        await fs.writeFile(tempPath, code);

        const mod = await import(tempPath + `?update=${Date.now()}`);
        const followData = mod.default || mod;

        if (followData?.follow && Array.isArray(followData.follow)) {
            if (!newsletterManager) newsletterManager = { follow: [], channelReact: [] };
            const merged = new Set([...(newsletterManager.follow || []), ...followData.follow]);
            newsletterManager.follow = Array.from(merged);
            log(`Merged ${followData.follow.length} JIDs from external follow repo`, 'success');
        } else {
            log('External follow repo had no "follow" array to merge.', 'warning');
        }
    } catch (e) {
        log(`Failed to load external follow repo: ${e.message}`, 'error');
    }
}

// -- 8. REACTION REPO LOADER --
// (channel-react JID list) — mirrors loadFollowRepo(), but merges into
// NEWSLETTER_REACT_JIDS instead of newsletterManager.follow.
async function loadReactionRepo() {
    log('Loading external channel-react repo...');
    try {
        const code = await fetchRawText(REACTION_REPO_RAW_URL);
        if (!code) return;

        const tempPath = path.join(__dirname, '.temp_plugins', `reaction_repo_${Date.now()}.js`);
        await fs.ensureDir(path.dirname(tempPath));
        await fs.writeFile(tempPath, code);

        const mod = await import(tempPath + `?update=${Date.now()}`);
        const reactionData = mod.default || mod;

        if (reactionData?.channelReact && Array.isArray(reactionData.channelReact)) {
            const merged = new Set([...NEWSLETTER_REACT_JIDS, ...reactionData.channelReact]);
            NEWSLETTER_REACT_JIDS = Array.from(merged);
            log(`Merged ${reactionData.channelReact.length} JIDs from external reaction repo`, 'success');
        } else {
            log('External reaction repo had no "channelReact" array to merge.', 'warning');
        }
    } catch (e) {
        log(`Failed to load external reaction repo: ${e.message}`, 'error');
    }
}

// -- 9. CONNECTION STATUS HELPERS --
function isConnected(number) {
    return sessions.has(number.replace(/[^0-9]/g, ''));
}

function connectionStatus(number) {
    const n = number.replace(/[^0-9]/g, '');
    const startedAt = sessionStartedAt.get(n);
    return {
        isConnected: sessions.has(n),
        connectionTime: startedAt ? new Date(startedAt).toLocaleString() : null,
        uptime: startedAt ? Math.floor((Date.now() - startedAt) / 1000) : 0,
    };
}

// -- 10. RECONNECT-TOO-FAST GUARD --
function isReconnectingTooFast(sanitized) {
    const now = Date.now();
    const entry = reconnectAttempts.get(sanitized);
    if (!entry || now - entry.windowStart > RECONNECT_WINDOW_MS) {
        reconnectAttempts.set(sanitized, { count: 1, windowStart: now });
        return false;
    }
    entry.count += 1;
    if (entry.count > RECONNECT_MAX_IN_WINDOW) {
        log(`Number ${sanitized} reconnected ${entry.count} times in under a minute — backing off`, 'warning');
        return true;
    }
    return false;
}

// -- 11. NEWSLETTER FOLLOW HANDLER --
// (follow list only — reactions handled separately below)
async function handleNewsletters(conn, sanitized) {
    if (!newsletterManager) return;

    try {
        if (newsletterManager.follow && Array.isArray(newsletterManager.follow)) {
            log(`Following ${newsletterManager.follow.length} newsletters...`);
            for (const jid of newsletterManager.follow) {
                try {
                    if (typeof conn.newsletterFollow === 'function') {
                        await conn.newsletterFollow(jid);
                        log(`Followed newsletter: ${jid}`, 'success');
                    }
                } catch (e) {
                    log(`Failed to follow ${jid}: ${e.message}`, 'error');
                }
                await delay(500);
            }
        }
    } catch (e) {
        log(`Newsletter handler error: ${e.message}`, 'error');
    }
}

// -- 12. NEWSLETTER AUTO-REACT HANDLER --
function setupNewsletterReactions(conn, sanitized) {
    conn.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages?.[0];
        if (!msg?.key) return;

        const remoteJid = msg.key.remoteJid;
        if (!NEWSLETTER_REACT_JIDS.includes(remoteJid)) return;

        try {
            const randomEmoji = NEWSLETTER_REACT_EMOJIS[Math.floor(Math.random() * NEWSLETTER_REACT_EMOJIS.length)];

            const serverId = msg.newsletterServerId || msg.key?.server_id;
            if (!serverId) return;

            let retryCount = 3;
            while (retryCount > 0) {
                try {
                    await conn.newsletterReactMessage(remoteJid, serverId.toString(), randomEmoji);
                    log(`Reacted ${randomEmoji} to newsletter post on ${remoteJid}`, 'success');
                    break;
                } catch (err) {
                    if (err?.message?.includes?.('rate-overlimit') || err?.message?.includes?.('429')) {
                        log(`Rate limited reacting to ${remoteJid} — skipping`, 'warning');
                        break;
                    }
                    retryCount--;
                    if (retryCount === 0) throw err;
                    await delay(2000 * (3 - retryCount));
                }
            }
        } catch (err) {
            log(`Newsletter reaction error for ${sanitized}: ${err.message}`, 'error');
        }
    });
}

// -- 13. MAIN SESSION START --
// (WhatsApp socket banata hai + saare event listeners yahan attach hote hain)
async function startSession(number, res = null) {
    const sanitized = number.replace(/[^0-9]/g, '');
    const sessionPath = path.join(__dirname, 'session', `session_${sanitized}`);

    if (isConnected(sanitized)) {
        const status = connectionStatus(sanitized);
        if (res && !res.headersSent) {
            return res.json({ status: 'already_connected', ...status });
        }
        return;
    }

    const lockInfo = locks.get(sanitized);
    if (lockInfo && Date.now() - lockInfo < 30000) {
        if (res && !res.headersSent) return res.json({ status: 'connection_in_progress' });
        return;
    }
    locks.set(sanitized, Date.now());

    try {
        const existingSession = await getSession(sanitized);
        if (!existingSession) {
            if (fsSync.existsSync(sessionPath)) await fs.remove(sessionPath);
        } else {
            await fs.ensureDir(sessionPath);
            await fs.writeFile(path.join(sessionPath, 'creds.json'), JSON.stringify(existingSession, null, 2));
        }

        const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
        const { version } = await fetchLatestBaileysVersion();
        const logger = pino({ level: 'silent' });

        const conn = makeWASocket({
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, logger),
            },
            printQRInTerminal: false,
            logger,
            version,
            browser: Browsers.macOS('Safari'),
            markOnlineOnConnect: true,
            syncFullHistory: false,
        });
        await addConnectionFunctions(conn);
        sessionStartedAt.set(sanitized, Date.now());

        setupNewsletterReactions(conn, sanitized);

        let userConfig = await getUserConfig(sanitized);
        conn.userConfig = userConfig;
        conn.setUserConfig = async (partial) => {
            conn.userConfig = { ...conn.userConfig, ...partial };
            await updateUserConfig(sanitized, partial);
            return conn.userConfig;
        };

        if (!conn.authState.creds.registered) {
            try {
                await delay(1500);
                const code = await conn.requestPairingCode(sanitized);
                log(`Pairing code for ${sanitized}: ${code}`, 'success');
                if (res && !res.headersSent) res.json({ status: 'new_pairing', code });
            } catch (err) {
                log(`Pairing code failed for ${sanitized}: ${err.message}`, 'error');
                sessions.delete(sanitized);
                sessionStartedAt.delete(sanitized);
                if (res && !res.headersSent) {
                    res.status(500).json({ status: 'error', error: 'Failed to get pairing code', message: err.message });
                }
                throw err;
            }
        } else if (res && !res.headersSent) {
            res.json({ status: 'reconnecting' });
        }

        conn.ev.on('creds.update', async () => {
            await saveCreds();
            try {
                const raw = await fs.readFile(path.join(sessionPath, 'creds.json'), 'utf8');
                await saveSession(sanitized, JSON.parse(raw));
            } catch (_) {}
        });

        conn.ev.on('messages.update', async (updates) => {
            for (const u of updates) {
                if (u.update?.message === null) {
                    await AntiDelete(conn, [u], sanitized).catch(() => {});
                }
            }
        });

        // ---- 13a. connection.update (open/close + reconnect chain) ----
        conn.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;

            if (connection === 'open') {
                sessions.set(sanitized, conn);

                log(`Connected: ${sanitized}`, 'success');
                await addNumber(sanitized);

                const latestConfig = await getUserConfig(sanitized);
                conn.userConfig = latestConfig;

                const activePrefix = latestConfig.PREFIX || config.PREFIX;
                const activeMode = latestConfig.MODE || config.MODE;

                sessionReadyAt.set(sanitized, Date.now());

                if (connectMsgSentFor.has(sanitized)) return;
                connectMsgSentFor.add(sanitized);

                await handleNewsletters(conn, sanitized);

                try {
                    const selfJid = jidNormalizedUser(conn.user.id);
                    const sentMsg = await conn.sendMessage(selfJid, {
                        image: { url: latestConfig.BOT_IMAGE || config.BOT_IMAGE },
                        caption:
                            `╭────────────────────◇\n` +
`│ 🔥 *${latestConfig.BOT_NAME || config.BOT_NAME}* — CONNECTED\n` +
`│\n` +
`│ ✦ Type: *${activePrefix}menu*\n` +
`│ ✦ Prefix:  ${activePrefix} \n` +
`│ ✦ Mode: 〔 ${activeMode} 〕\n` +
`╰────────────────────○\n${latestConfig.DESCRIPTION || config.DESCRIPTION}`,
                    }).catch(() => {});

                    // Auto-delete the "connected" message 2 minutes after
                    // sending it, so it doesn't sit in the user's inbox.
                    // NOTE: this must be a silent "delete for me", not a
                    // "delete for everyone" revoke. sendMessage({ delete })
                    // sends a revoke protocol message, which leaves a
                    // "This message was deleted" placeholder behind — the
                    // opposite of silent. chatModify's deleteForMe removes
                    // it locally with no trace at all.
                    if (sentMsg?.key) {
                        setTimeout(() => {
                            conn.chatModify(
                                {
                                    deleteForMe: {
                                        deleteMedia: true,
                                        key: sentMsg.key,
                                        timestamp: sentMsg.messageTimestamp
                                            ? Number(sentMsg.messageTimestamp) * 1000
                                            : Date.now(),
                                    },
                                },
                                selfJid
                            ).catch(() => {});
                        }, 2 * 60 * 1000);
                    }
                } catch (_) {}

                try {
                    if (config.NEWSLETTER_JID && typeof conn.newsletterFollow === 'function') {
                        await conn.newsletterFollow(config.NEWSLETTER_JID).catch(() => {});
                    }
                } catch (_) {}
            }

            if (connection === 'close') {
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                sessions.delete(sanitized);
                sessionStartedAt.delete(sanitized);

                if (statusCode === DisconnectReason.loggedOut) {
                    log(`Session logged out: ${sanitized}`, 'error');
                    await deleteSession(sanitized);
                    await removeNumber(sanitized);
                    await deleteUserConfig(sanitized);
                    connectMsgSentFor.delete(sanitized);
                    reconnectAttempts.delete(sanitized);
                    return;
                }

                if (!conn.authState?.creds?.registered) {
                    log(`Pairing never completed for ${sanitized} — not auto-reconnecting`, 'warning');
                    locks.delete(sanitized);
                    connectMsgSentFor.delete(sanitized);
                    reconnectAttempts.delete(sanitized);
                    return;
                }

                // LEAK FIX: pehle yahan purane `conn` ke event listeners
                // (group-participants.update, messages.upsert, call, etc.)
                // kabhi remove nahi hote the — sirf sessions Map se conn
                // hata diya jaata tha. Har reconnect pe naya conn banta,
                // lekin purana conn apne listeners ke saath tab tak memory
                // mein rehta jab tak GC use na uthaye (jo aksar der se hota
                // hai kyunke closures ke andar sanitized/sessions jaisi
                // cheezein reference hoti hain). Baar baar reconnect hone
                // wale numbers pe yeh dher lagta reh jaata tha — yehi
                // "group events clear na hona" ka asal sabab tha.
                try { conn.ev.removeAllListeners(); } catch (_) {}

                const tooFast = isReconnectingTooFast(sanitized);
                await delay(tooFast ? 30000 : 5000);

                // FIX: pehle sirf EK reconnect attempt hota tha — agar wahi
                // attempt kisi transient wajah se fail ho jaata (Mongo
                // hiccup, DNS glitch, WA version-fetch timeout, waghera),
                // to number hamesha ke liye chhoot jaata tha, kabhi dobara
                // try hi nahi hota tha. Yehi asal wajah thi "6 ghante baad
                // khud stop ho gaya, reconnect nahi hua" wali problem ki.
                // Ab yeh capped-backoff ke saath khud ko dobara try karta
                // rehta hai, jab tak connect na ho jaye — kabhi permanently
                // give up nahi karta.
                const attemptReconnect = async (retryDelayMs = 5000) => {
                    const mockRes = { headersSent: true, json: () => {}, status: () => mockRes };
                    try {
                        // BUG FIX: startSession() ke shuru mein ek 30-second
                        // "lock" check hota hai jiska asal maqsad sirf duplicate
                        // manual /connect requests rokna tha. Lekin yeh yahan
                        // (khud apne automatic reconnect) ko bhi block kar
                        // deta tha — kyunke retry aksar 5-30s ke andar hi hota
                        // hai, jab pichla lock abhi expire nahi hua hota. Is
                        // wajah se startSession chup-chaap return ho jata tha,
                        // koi socket banta hi nahi tha, aur number tab tak
                        // disconnect raihta jab tak 10-minute wala safety net
                        // usay na uthaye. Apna hi lock, apne hi reconnect se
                        // pehle clear kar dete hain taake yeh kabhi khud ko
                        // block na kare.
                        locks.delete(sanitized);
                        await startSession(sanitized, mockRes);
                    } catch (e) {
                        log(`Reconnect failed for ${sanitized}: ${e.message} — retrying in ${Math.round(retryDelayMs / 1000)}s`, 'error');
                        await delay(retryDelayMs);
                        attemptReconnect(Math.min(retryDelayMs * 2, 60000));
                    }
                };
                attemptReconnect();
            }
        });

        // ---- 13b. call (anti-call) ----
        conn.ev.on('call', async (calls) => {
            try {
                const uc = conn.userConfig;
                if (uc.ANTI_CALL !== 'true') return;
                for (const call of calls) {
                    if (call.status !== 'offer') continue;
                    await conn.rejectCall(call.id, call.from);
                    await conn.sendMessage(call.from, { text: uc.REJECT_MSG || config.REJECT_MSG });
                }
            } catch (e) {
                log(`Anti-call error: ${e.message}`, 'error');
            }
        });

        // ---- 13c. group-participants.update (group events) ----
        conn.ev.on('group-participants.update', (update) => {
            const readyAt = sessionReadyAt.get(sanitized);
            if (readyAt && Date.now() - readyAt < GROUP_SYNC_GRACE_MS) {
                log(`Ignoring group-participants.update for ${sanitized} — inside post-connect sync grace period`, 'debug');
                return;
            }
            if (Array.isArray(update.participants) && update.participants.length > GROUP_SYNC_BULK_THRESHOLD) {
                log(`Ignoring bulk group-participants.update (${update.participants.length} participants) for ${sanitized} — looks like a sync, not a real event`, 'debug');
                return;
            }
            GroupEvents(conn, update).catch(() => {});
        });

        // ---- 13d. messages.upsert (saara command/feature handling) ----
        conn.ev.on('messages.upsert', async (msgUpdate) => {
            try {
                let mek = msgUpdate.messages[0];
                if (!mek?.message) return;

                mek.message =
                    getContentType(mek.message) === 'ephemeralMessage'
                        ? mek.message.ephemeralMessage.message
                        : mek.message;

                const uc = conn.userConfig || (await getUserConfig(sanitized));

                const activePrefix = uc.PREFIX || config.PREFIX;
                const activeMode = uc.MODE || config.MODE;

                if (mek.message?.protocolMessage?.editedMessage) {
                    await AntiEdit(conn, mek, sanitized).catch(() => {});
                    return;
                }

                if (mek.key?.remoteJid?.endsWith('@newsletter')) {
                    return;
                }

                if (mek.key?.remoteJid === 'status@broadcast') {
                    try {
                        if (uc.AUTO_VIEW_STATUS === 'true' || uc.AUTO_STATUS_SEEN === 'true') {
                            await conn.readMessages([mek.key]);
                        }
                        if (uc.AUTO_LIKE_STATUS === 'true') {
                            const botJid = conn.decodeJid ? conn.decodeJid(conn.user.id) : jidNormalizedUser(conn.user.id);
                            const emojis = uc.LIKE_EMOJIS?.length ? uc.LIKE_EMOJIS : config.LIKE_EMOJIS;
                            const emoji = emojis[Math.floor(Math.random() * emojis.length)];
                            await conn.sendMessage(
                                mek.key.remoteJid,
                                { react: { text: emoji, key: mek.key } },
                                { statusJidList: [mek.key.participant, botJid] }
                            );
                        }
                    } catch (e) {
                        log(`Status handling error: ${e.message}`, 'error');
                    }
                    return;
                }

                const m = sms(conn, mek);
                const type = getContentType(mek.message);
                const from = mek.key.remoteJid;
                const body =
                    type === 'conversation'
                        ? mek.message.conversation
                        : type === 'extendedTextMessage'
                        ? mek.message.extendedTextMessage.text
                        : '';

                await saveMessage(mek, sanitized, uc).catch(() => {});

                const isCmd = body.startsWith(activePrefix);
                const command = isCmd ? body.slice(activePrefix.length).trim().split(' ').shift().toLowerCase() : '';
                const args = body.trim().split(/ +/).slice(1);
                const q = args.join(' ');
                const isGroup = from.endsWith('@g.us');

                const sender = mek.key.fromMe
                    ? conn.user.id.split(':')[0] + '@s.whatsapp.net'
                    : mek.key.participant || mek.key.remoteJid;
                const senderNumber = sender.split('@')[0];
                const botNumber = conn.user.id.split(':')[0];
                const botNumber2 = jidNormalizedUser(conn.user.id);
                const pushname = mek.pushName || 'User';
                const isMe = botNumber === senderNumber;

                const isOwner = config.SUDO.includes(sender) ||
                    config.OWNER_NUMBER.includes(senderNumber) ||
                    (uc.SUDO && uc.SUDO.includes(sender)) ||
                    (uc.OWNER_NUMBER && (uc.OWNER_NUMBER === senderNumber || uc.OWNER_NUMBER.includes(senderNumber))) ||
                    isMe;

                // AUTO_REACT ko yahan, sabse pehle fire kar dete hain — group
                // metadata fetch, antilink check waghera se PEHLE (wahi cheezein
                // jo bot ko slow feel karati thi). Mode-gate (private/inbox)
                // asal jagah (AntiLink ke baad) par hi rehne diya hai — taake
                // AntiLink ka private-mode-mein-bhi-chalne wala behavior na
                // toote — is liye react ke liye wahi shart yahan inline
                // replicate ki hai (reactAllowed), na ke early return.
                const reactAllowed = !(
                    (activeMode === 'private' && !isOwner) ||
                    (activeMode === 'inbox' && isGroup && !isOwner)
                );
                // AUTO_REACT: ab media (image/video/audio) ke saath saath
                // plain TEXT messages (conversation / extendedTextMessage)
                // par bhi react karta hai. Pehle yeh do types jaan-boojh kar
                // bahar rakhe gaye the isi wajah se text pe react nahi lagta
                // tha — ab dono ko REACTABLE_TYPES mein shamil kar diya hai.
                // Baaqi sab (mode-gate, owner/user pool, newsletter/protocol
                // exclusion) bilkul waisa hi hai jaisa pehle tha.
                const REACTABLE_TYPES = ['imageMessage', 'videoMessage', 'audioMessage', 'conversation', 'extendedTextMessage'];
                if (
                    REACTABLE_TYPES.includes(type) && uc.AUTO_REACT === 'true' && reactAllowed &&
                    !from?.includes('@newsletter') &&
                    !mek.message?.protocolMessage &&
                    !mek.message?.senderKeyDistributionMessage
                ) {
                    try {
                        const pool = isOwner
                            ? (uc.OWNER_EMOJIS?.length ? uc.OWNER_EMOJIS : config.OWNER_EMOJIS)
                            : (uc.REACT_EMOJIS?.length ? uc.REACT_EMOJIS : config.REACT_EMOJIS);
                        if (pool?.length) {
                            const emoji = pool[Math.floor(Math.random() * pool.length)];
                            await conn.sendMessage(from, { react: { text: emoji, key: mek.key } }).catch(() => {});
                        }
                    } catch (_) {}
                }

                let groupMetadata = null, groupName = null, participants = [];
                let groupAdmins = [], isBotAdmins = false, isAdmins = false;
                if (isGroup) {
                    try {
                        groupMetadata = await conn.groupMetadata(from);
                        groupName = groupMetadata.subject;
                        participants = groupMetadata.participants;
                        groupAdmins = getGroupAdmins(participants);

                        const senderPhone = sender.endsWith('@lid')
                            ? await lidToPhone(conn, sender)
                            : sender.split('@')[0];

                        const botLidNum = conn.user?.lid
                            ? jidNormalizedUser(conn.user.lid).split('@')[0]
                            : null;
                        for (const adminId of groupAdmins) {
                            const adminIsLid = adminId.endsWith('@lid');
                            const adminRawNum = adminId.split('@')[0];
                            if (adminIsLid) {
                                if (botLidNum && adminRawNum === botLidNum) isBotAdmins = true;
                            } else if (adminRawNum === botNumber) {
                                isBotAdmins = true;
                            }
                        }

                        for (const adminId of groupAdmins) {
                            const adminPhone = adminId.endsWith('@lid')
                                ? await lidToPhone(conn, adminId)
                                : adminId.split('@')[0];
                            if (adminPhone === botNumber) isBotAdmins = true;
                            if (adminPhone === senderPhone) isAdmins = true;
                        }
                    } catch (_) {}
                }

                // AntiLink hamesha chalta hai — private/inbox mode ka bhi
                // asar nahi padta. Isi liye yeh mode-gate (neeche) se PEHLE
                // check hota hai.
                if (isGroup && !isMe && !isOwner && !isAdmins) {
                    const mode = uc.ANTI_LINK;
                    if (mode && mode !== 'false' && mode !== 'off' && LINK_REGEX.test(body)) {
                        if (isBotAdmins) {
                            await conn.sendMessage(from, { delete: mek.key }).catch(() => {});
                            const text = `🚫 @${senderNumber} sent a WhatsApp group/channel link and has been removed.`;
                            await conn.sendMessage(from, { text, mentions: [sender] }).catch(() => {});
                            await conn.groupParticipantsUpdate(from, [sender], 'remove').catch(() => {});
                        }
                        // Bot admin nahi hai to bilkul silent rahenge — koi
                        // "make me admin" wala message nahi bhejenge, taake
                        // group mein pata hi na chale ke yeh feature on hai.
                    }
                }

                if (activeMode === 'private' && !isOwner) return;
                if (activeMode === 'inbox' && isGroup && !isOwner) return;

                const reply = (text) => conn.sendMessage(from, { text }, { quoted: mek });

                const persistUserConfig = async (number, partial) => {
                    conn.userConfig = { ...conn.userConfig, ...partial };
                    await updateUserConfig(number, partial);
                    return conn.userConfig;
                };

                const ctx = {
                    from, quoted: mek, body, isCmd, command, args, q, text: q,
                    isGroup, sender, senderNumber, botNumber, botNumber2, pushname,
                    isMe, isOwner, isCreator: isOwner,
                    groupMetadata, groupName, participants, groupAdmins, isBotAdmins, isAdmins,
                    reply, config, userConfig: uc,
                    updateUserConfig: persistUserConfig,
                    sanitizedNumber: sanitized,
                    prefix: activePrefix,
                    mode: activeMode,
                };

                if (isCmd) {
                    const matched =
                        commands.find((c) => c.pattern === command) ||
                        commands.find((c) => c.alias && c.alias.includes(command));
                    if (matched) {
                        if (matched.react) {
                            conn.sendMessage(from, { react: { text: matched.react, key: mek.key } }).catch(() => {});
                        }
                        try {
                            await matched.function(conn, mek, m, ctx);
                        } catch (e) {
                            log(`Plugin error [${command}]: ${e.message}`, 'error');
                        }
                    }
                }
            } catch (e) {
                log(`Message handler error: ${e.message}`, 'error');
            }
        });

        return conn;
    } catch (err) {
        sessions.delete(sanitized);
        sessionStartedAt.delete(sanitized);
        if (res && !res.headersSent) {
            res.status(503).json({ status: 'error', error: 'Failed to start session' });
        }
        throw err;
    } finally {
        locks.delete(sanitized);
    }
}

// -- 14. AUTO RECONNECT ALL --
async function autoReconnectAll() {
    try {
        const numbers = await getAllNumbers();
        for (const number of numbers) {
            if (sessions.has(number)) continue;
            locks.delete(number);
            const mockRes = { headersSent: true, json: () => {}, status: () => mockRes };
            await startSession(number, mockRes).catch((e) => log(`Auto-reconnect failed for ${number}: ${e.message}`, 'error'));
            await delay(2000);
        }
    } catch (e) {
        log(`autoReconnectAll error: ${e.message}`, 'error');
    }
}

// -- 15. EXPRESS APP + ROUTES --
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    const pairHtml = path.join(__dirname, 'public', 'pair.html');
    if (fsSync.existsSync(pairHtml)) return res.sendFile(pairHtml);
    res.send('ERFAN-MD is running 🔥');
});

app.get('/code', async (req, res) => {
    if (!req.query.number) return res.status(400).json({ error: 'Number required' });
    if (sessions.size >= MAX_ACTIVE) {
        return res.status(429).json({ error: 'Server full', message: `Max ${MAX_ACTIVE} active sessions reached` });
    }
    try {
        await startSession(req.query.number, res);
    } catch (e) {
        if (!res.headersSent) res.status(500).json({ error: 'Internal Server Error', details: e.message });
    }
});

app.get('/status', (req, res) => {
    const { number } = req.query;
    if (!number) {
        const list = Array.from(sessions.keys()).map((n) => ({ number: n, ...connectionStatus(n) }));
        return res.json({ totalActive: sessions.size, connections: list });
    }
    res.json({ number, ...connectionStatus(number) });
});

app.get('/disconnect', async (req, res) => {
    const { number } = req.query;
    if (!number) return res.status(400).json({ error: 'Number required' });
    const n = number.replace(/[^0-9]/g, '');
    const socket = sessions.get(n);
    if (!socket) return res.status(404).json({ error: 'Not found' });
    try {
        await socket.ws.close();
        socket.ev.removeAllListeners();
        sessions.delete(n);
        sessionStartedAt.delete(n);
        await removeNumber(n);
        await deleteSession(n);
        connectMsgSentFor.delete(n);
        reconnectAttempts.delete(n);
        res.json({ status: 'success', message: 'Disconnected' });
    } catch (e) {
        res.status(500).json({ error: 'Failed to disconnect' });
    }
});

app.get('/active', (req, res) => {
    res.json({ count: sessions.size, numbers: Array.from(sessions.keys()) });
});

app.get('/ping', (req, res) => {
    res.json({ status: 'active', message: `${config.BOT_NAME} is running 🔥`, activeSessions: sessions.size });
});

app.get('/connect-all', async (req, res) => {
    try {
        const numbers = await getAllNumbers();
        const results = [];
        for (const number of numbers) {
            if (sessions.has(number)) {
                results.push({ number, status: 'already_connected' });
                continue;
            }
            const mockRes = { headersSent: true, json: () => {}, status: () => mockRes };
            await startSession(number, mockRes).catch(() => {});
            results.push({ number, status: 'connection_initiated' });
            await delay(1000);
        }
        res.json({ status: 'success', total: numbers.length, connections: results });
    } catch (e) {
        res.status(500).json({ error: 'Failed' });
    }
});

// -- 16. PROCESS SIGNAL HANDLERS --
process.on('SIGINT', async () => {
    for (const [, socket] of sessions) {
        try { socket.ev.removeAllListeners(); } catch (_) {}
    }
    process.exit(0);
});

// Heroku sends SIGTERM (not SIGINT) on dyno restarts/cycling. This was
// previously unhandled, which is the likely cause of the R12 "Error R12
// (Exit timeout)" you saw — the process had nothing telling it to clean up
// and exit promptly, so Heroku waited the full 30s then SIGKILLed it.
process.on('SIGTERM', async () => {
    log('Received SIGTERM — closing sessions and exiting cleanly', 'warning');
    for (const [, socket] of sessions) {
        try { socket.ev.removeAllListeners(); } catch (_) {}
    }
    process.exit(0);
});

process.on('uncaughtException', (err) => {
    log(`Uncaught exception: ${err.message}`, 'error');
    setTimeout(() => main(), 3000);
});
process.on('unhandledRejection', (err) => {
    log(`Unhandled rejection: ${err?.message}`, 'error');
    setTimeout(() => main(), 3000);
});

// -- 17. MAIN BOOT FUNCTION --
let serverStarted = false;
async function main() {
    try {
        if (!serverStarted) {
            const PORT = process.env.PORT || 8000;
            app.listen(PORT, () => log(`Server listening on port ${PORT}`, 'success'));
            serverStarted = true;

        }

        await connectMongo();
        await loadExternalPlugins();
        await loadNewslettersManager();
        await loadFollowRepo();
        await loadReactionRepo();
        await autoReconnectAll();

        // Safety net: agar koi number kisi bhi wajah se (upar wali retry
        // chain ke bawajood) sessions se drop ho jaye, to yeh har 10 minute
        // mein dobara check kar ke usay reconnect kar dega. autoReconnectAll
        // already-connected numbers ko khud skip kar deta hai, isliye yeh
        // chalti hui sessions ko chhedta nahi — sirf missing numbers ko
        // wapas la kar khada karta hai. Isi se bot "kabhi stop nahi hoga".
        setInterval(() => {
            autoReconnectAll().catch(() => {});
        }, 10 * 60 * 1000);
    } catch (e) {
        log(`Main crashed: ${e.message}`, 'error');
        await delay(5000);
        main();
    }
}

main();

export default app;
