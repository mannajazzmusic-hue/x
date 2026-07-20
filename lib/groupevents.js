// lib/groupevents.js - ESM Version
// Credits ERFAN - ERFAN-MD 


import { isJidGroup } from '@whiskeysockets/baileys';
import config from '../config.js';
import { lidToPhone } from './functions.js';

// Add delay between messages to avoid rate limits
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// FIX (NEW - SPAM): tracks last time we actually sent a message for a given
// group+action combo, to prevent rapid repeat firing from spamming the group.
const lastSentAt = new Map(); // key: `${groupId}:${action}` -> timestamp
const COOLDOWN_MS = 4000; // ignore repeat triggers for same group+action within 4s

const GroupEvents = async (conn, update) => {
    try {
        // Null check for update and update.id
        if (!update || !update.id) return;
        
        const isGroup = isJidGroup(update.id);
        if (!isGroup) return;

        // Null check for update.participants
        if (!update.participants || !Array.isArray(update.participants) || update.participants.length === 0) return;

        // FIX (NEW - SPAM): cooldown guard — if we just handled this exact
        // group+action combo a moment ago, skip. This stops back-to-back
        // duplicate events from producing a burst of messages.
        const cooldownKey = `${update.id}:${update.action}`;
        const now = Date.now();
        const lastTime = lastSentAt.get(cooldownKey);
        if (lastTime && now - lastTime < COOLDOWN_MS) {
            console.log(`[⏱️] Skipping duplicate/rapid ${update.action} event for ${update.id} — within cooldown`);
            return;
        }
        lastSentAt.set(cooldownKey, now);

        // Get userConfig from connection object
        const userConfig = conn.userConfig || { ...config.DEFAULT_SETTINGS };

        // Get metadata with error handling for rate limits
        let metadata;
        try {
            metadata = await conn.groupMetadata(update.id);
        } catch (err) {
            // If rate limit hit, skip this group event completely
            if (err.message?.includes('rate-overlimit') || err.message?.includes('429')) {
                console.log(`[⏱️] Group event rate limit: ${update.id} - skipping`);
                return;
            }
            // Other errors: log and skip
            console.error('Group metadata error:', err.message);
            return;
        }
        
        // Null check for metadata
        if (!metadata) return;

        const participants = update.participants;
        const desc = metadata.desc || "No Description";
        const groupMembersCount = metadata.participants ? metadata.participants.length : 0;
        const timestamp = new Date().toLocaleString();

        // FIX (NEW - SPAM): resolve names for all participants first, then send
        // ONE combined message for the whole batch instead of looping+sending
        // a separate message per participant.
        const resolvedUsers = [];
        for (const user of participants) {
            if (!user) continue;
            const lid = user.id || user;
            if (!lid) continue;

            let userName;
            try {
                const userPN = await lidToPhone(conn, lid);
                userName = userPN || lid.split('@')[0] || "unknown";
            } catch (e) {
                userName = lid.split('@')[0] || "unknown";
            }
            resolvedUsers.push({ lid, userName });
        }

        if (resolvedUsers.length === 0) return;

        // Build a combined "@user" mention string, e.g. "@92300..., @92301..."
        const combinedUserMentionText = resolvedUsers.map(u => `@${u.userName}`).join(', ');
        const combinedMentionJids = resolvedUsers.map(u => u.lid);

        try {
            if (update.action === "add" && userConfig.WELCOME === "true") {
                const welcomeMessageTemplate = userConfig.WELCOME_MESSAGE || config.WELCOME_MESSAGE;
                if (!welcomeMessageTemplate) return;

                let welcomeMsg = welcomeMessageTemplate
                    .replace(/@user/g, combinedUserMentionText)
                    .replace(/@group/g, metadata.subject || "Group")
                    .replace(/@desc/g, desc)
                    .replace(/@count/g, groupMembersCount)
                    .replace(/@bot/g, userConfig.BOT_NAME || config.BOT_NAME || "Bot")
                    .replace(/@time/g, timestamp);

                await conn.sendMessage(update.id, {
                    text: welcomeMsg,
                    mentions: combinedMentionJids
                });

            } else if (update.action === "remove" && userConfig.GOODBYE === "true") {
                const goodbyeMessageTemplate = userConfig.GOODBYE_MESSAGE || config.GOODBYE_MESSAGE;
                if (!goodbyeMessageTemplate) return;

                let goodbyeMsg = goodbyeMessageTemplate
                    .replace(/@user/g, combinedUserMentionText)
                    .replace(/@group/g, metadata.subject || "Group")
                    .replace(/@desc/g, desc)
                    .replace(/@count/g, groupMembersCount)
                    .replace(/@bot/g, userConfig.BOT_NAME || config.BOT_NAME || "Bot")
                    .replace(/@time/g, timestamp);

                await conn.sendMessage(update.id, {
                    text: goodbyeMsg,
                    mentions: combinedMentionJids
                });

            } else if (update.action === "demote" && userConfig.ADMIN_ACTION === "true") {
                if (!update.author) return;

                const authorLid = update.author;
                let authorName;
                try {
                    const authorPN = await lidToPhone(conn, authorLid);
                    authorName = authorPN || authorLid.split('@')[0] || "unknown";
                } catch (e) {
                    authorName = authorLid.split('@')[0] || "unknown";
                }

                await conn.sendMessage(update.id, {
                    text: `@${authorName} demoted ${combinedUserMentionText}`,
                    mentions: [authorLid, ...combinedMentionJids]
                });

            } else if (update.action === "promote" && userConfig.ADMIN_ACTION === "true") {
                if (!update.author) return;

                const authorLid = update.author;
                let authorName;
                try {
                    const authorPN = await lidToPhone(conn, authorLid);
                    authorName = authorPN || authorLid.split('@')[0] || "unknown";
                } catch (e) {
                    authorName = authorLid.split('@')[0] || "unknown";
                }

                await conn.sendMessage(update.id, {
                    text: `@${authorName} promoted ${combinedUserMentionText}`,
                    mentions: [authorLid, ...combinedMentionJids]
                });
            }

        } catch (err) {
            if (err.message?.includes('rate-overlimit') || err.message?.includes('429')) {
                console.log(`[⏱️] Rate limit hit for ${update.action}, skipping`);
            } else {
                console.error(`Error sending ${update.action} message:`, err.message);
            }
        }
    } catch (err) {
        // Silent fail for outer errors
        if (!err.message?.includes('rate-overlimit')) {
            console.error('Group event error:', err.message);
        }
    }
};

export default GroupEvents;
