// lib/mongo.js - ESM MongoDB session & config storage
import { MongoClient } from 'mongodb';
import config from '../config.js';

let client;
let db;

export async function connectMongo() {
    if (db) return db;
    client = new MongoClient(config.MONGODB_URL);
    await client.connect();
    db = client.db(config.DB_NAME);
    await db.collection(config.COLLECTIONS.SESSIONS).createIndex({ number: 1 }, { unique: true });
    await db.collection(config.COLLECTIONS.NUMBERS).createIndex({ number: 1 }, { unique: true });
    await db.collection(config.COLLECTIONS.CONFIGS).createIndex({ number: 1 }, { unique: true });
    console.log('✅ MongoDB connected');
    return db;
}

const clean = (n) => n.replace(/[^0-9]/g, '');

export async function saveSession(number, creds) {
    try {
        const n = clean(number);
        await db.collection(config.COLLECTIONS.SESSIONS).updateOne(
            { number: n },
            { $set: { number: n, creds, updatedAt: new Date() } },
            { upsert: true }
        );
        return true;
    } catch (e) {
        console.log('saveSession error:', e.message);
        return false;
    }
}

export async function getSession(number) {
    try {
        const n = clean(number);
        const doc = await db.collection(config.COLLECTIONS.SESSIONS).findOne({ number: n });
        return doc ? doc.creds : null;
    } catch (e) {
        return null;
    }
}

export async function deleteSession(number) {
    try {
        const n = clean(number);
        await db.collection(config.COLLECTIONS.SESSIONS).deleteOne({ number: n });
        return true;
    } catch (e) {
        return false;
    }
}

export async function addNumber(number) {
    try {
        const n = clean(number);
        await db.collection(config.COLLECTIONS.NUMBERS).updateOne(
            { number: n },
            { $set: { number: n, lastConnected: new Date(), isActive: true } },
            { upsert: true }
        );
        return true;
    } catch (e) {
        return false;
    }
}

export async function removeNumber(number) {
    try {
        const n = clean(number);
        await db.collection(config.COLLECTIONS.NUMBERS).deleteOne({ number: n });
        return true;
    } catch (e) {
        return false;
    }
}

export async function getAllNumbers() {
    try {
        const docs = await db.collection(config.COLLECTIONS.NUMBERS).find({ isActive: true }).toArray();
        return docs.map((d) => d.number);
    } catch (e) {
        return [];
    }
}

export async function getUserConfig(number) {
    try {
        const n = clean(number);
        const doc = await db.collection(config.COLLECTIONS.CONFIGS).findOne({ number: n });
        if (doc && doc.settings) return { ...config.DEFAULT_SETTINGS, ...doc.settings };
        const defaults = { ...config.DEFAULT_SETTINGS };
        await saveUserConfig(n, defaults);
        return defaults;
    } catch (e) {
        return { ...config.DEFAULT_SETTINGS };
    }
}

export async function saveUserConfig(number, settings) {
    try {
        const n = clean(number);
        await db.collection(config.COLLECTIONS.CONFIGS).updateOne(
            { number: n },
            { $set: { number: n, settings, updatedAt: new Date() } },
            { upsert: true }
        );
        return true;
    } catch (e) {
        console.log('saveUserConfig error:', e.message);
        return false;
    }
}

export async function updateUserConfig(number, partial) {
    const current = await getUserConfig(number);
    const merged = { ...current, ...partial };
    await saveUserConfig(number, merged);
    return merged;
}

// FIX: YEH FUNCTION ADD KARO — jab device delete ho toh settings bhi delete ho
export async function deleteUserConfig(number) {
    try {
        const n = clean(number);
        await db.collection(config.COLLECTIONS.CONFIGS).deleteOne({ number: n });
        console.log(`🗑️ User config deleted for ${n}`);
        return true;
    } catch (e) {
        console.log('deleteUserConfig error:', e.message);
        return false;
    }
}

export function getDb() {
    return db;
}
