/**
 * server-whatsapp.js
 *
 * Standalone 24/7 WhatsApp Background Worker.
 * Designed to run on persistent hosting platforms (Render, Railway, Fly.io, or VPS)
 * to keep Baileys WebSockets perpetually active across all connected gym sessions.
 *
 * Features:
 * 1. Automatic session sync: Queries Supabase `whatsapp_sessions` and maintains live sockets.
 * 2. Self-healing reconnects: Auto-reconnects on network drops without losing authentication.
 * 3. HTTP Health & Control API: Exposes /health and /api/send for external triggers.
 * 4. Graceful shutdown: Handles SIGTERM/SIGINT safely without corrupting session data.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const {
  default: makeWASocket,
  DisconnectReason,
  BufferJSON,
  initAuthCreds,
  makeCacheableSignalKeyStore,
  proto,
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const pino = require('pino');

// ── 1. Load Environment Variables (Fallback for local testing) ─────────────
function loadEnv() {
  const envFiles = ['.env.local', '.env'];
  for (const file of envFiles) {
    const fullPath = path.resolve(process.cwd(), file);
    if (fs.existsSync(fullPath)) {
      const content = fs.readFileSync(fullPath, 'utf8');
      content.split('\n').forEach((line) => {
        const trimmed = line.trim();
        if (trimmed && !trimmed.startsWith('#')) {
          const eqIdx = trimmed.indexOf('=');
          if (eqIdx !== -1) {
            const key = trimmed.slice(0, eqIdx).trim();
            const val = trimmed.slice(eqIdx + 1).trim().replace(/^['"]|['"]$/g, '');
            if (!process.env[key]) {
              process.env[key] = val;
            }
          }
        }
      });
    }
  }
}
loadEnv();

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PORT = process.env.PORT || 3001;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('[WhatsApp Worker] FATAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const activeSockets = globalThis.__worker_active_sockets || (globalThis.__worker_active_sockets = new Map()); // gymId -> { socket, status, lastSeen }
const connectingPromises = globalThis.__worker_connecting_promises || (globalThis.__worker_connecting_promises = new Map()); // gymId -> Promise<socket> (Singleton Mutex)
const reconnectTimers = globalThis.__worker_reconnect_timers || (globalThis.__worker_reconnect_timers = new Map()); // gymId -> Timeout
const reconnectAttempts = globalThis.__worker_reconnect_attempts || (globalThis.__worker_reconnect_attempts = new Map()); // gymId -> number
const MAX_RECONNECT_ATTEMPTS = 5;

// Queues for sequential message dispatching with safety delay
const messageQueues = globalThis.__worker_message_queues || (globalThis.__worker_message_queues = new Map()); // gymId -> Array of queued messages
const isProcessingQueue = globalThis.__worker_processing_queue || (globalThis.__worker_processing_queue = new Map()); // gymId -> boolean

// Status cache for debounced/throttled status polling requests
const statusCache = new Map(); // gymId -> { data, timestamp }

// Helper to guarantee that socket.ws.readyState evaluates cleanly to 1 when open
function ensureSocketReadyState(socket) {
  if (!socket?.ws) return;
  if (typeof socket.ws.readyState !== 'number') {
    try {
      Object.defineProperty(socket.ws, 'readyState', {
        get() {
          if (typeof this.socket?.readyState === 'number') {
            return this.socket.readyState;
          }
          return this.isOpen ? 1 : 0;
        },
        configurable: true,
      });
    } catch (_) {}
  }
}

// Helper to reliably check if Baileys WebSocket is open across all versions/wrappers
function isSocketWsOpen(socket) {
  if (!socket?.ws) return false;
  ensureSocketReadyState(socket);
  return Boolean(
    socket.ws.readyState === 1 ||
    socket.ws.socket?.readyState === 1 ||
    socket.ws.isOpen === true
  );
}

function getSocketReadyState(socket) {
  if (!socket?.ws) return null;
  ensureSocketReadyState(socket);
  if (typeof socket.ws.readyState === 'number') return socket.ws.readyState;
  if (typeof socket.ws.socket?.readyState === 'number') return socket.ws.socket.readyState;
  if (socket.ws.isOpen === true) return 1;
  return 0;
}

// ── 2. Supabase Auth State Adapter for Worker ──────────────────────────────
async function getWorkerAuthState(gymId) {
  const { data } = await supabase
    .from('whatsapp_sessions')
    .select('session_data')
    .eq('gym_id', gymId)
    .maybeSingle();

  let creds;
  let sessionKeys = {};

  if (data?.session_data) {
    try {
      const parsed = JSON.parse(JSON.stringify(data.session_data), BufferJSON.reviver);
      creds = parsed.creds || initAuthCreds();
      sessionKeys = parsed.keys || {};
    } catch (err) {
      console.error(`[Worker] Error reviving session for gym ${gymId}:`, err);
      creds = initAuthCreds();
    }
  } else {
    creds = initAuthCreds();
  }

  const writeSessionToDb = async () => {
    try {
      const payload = JSON.parse(
        JSON.stringify({ creds, keys: sessionKeys }, BufferJSON.replacer)
      );

      await supabase
        .from('whatsapp_sessions')
        .upsert(
          {
            gym_id: gymId,
            session_data: payload,
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'gym_id' }
        );
    } catch (err) {
      console.error(`[Worker] Error saving creds to DB for gym ${gymId}:`, err);
    }
  };

  const rawKeyStore = {
    get: async (type, ids) => {
      const result = {};
      for (const id of ids) {
        let value = sessionKeys[`${type}-${id}`];
        if (type === 'app-state-sync-key' && value) {
          value = proto.Message.AppStateSyncKeyData.fromObject(value);
        }
        if (value) result[id] = value;
      }
      return result;
    },
    set: async (data) => {
      let hasChanges = false;
      for (const category in data) {
        for (const id in data[category]) {
          const value = data[category][id];
          const keyName = `${category}-${id}`;
          if (value) {
            sessionKeys[keyName] = value;
          } else {
            delete sessionKeys[keyName];
          }
          hasChanges = true;
        }
      }
      if (hasChanges) await writeSessionToDb();
    },
    clear: async () => {
      sessionKeys = {};
      await supabase.from('whatsapp_sessions').delete().eq('gym_id', gymId);
    },
  };

  return {
    state: {
      creds,
      keys: makeCacheableSignalKeyStore(rawKeyStore),
    },
    saveCreds: writeSessionToDb,
  };
}

// ── 3. Start Socket for a Gym (Singleton Mutex via createBaileysSocket) ─────
/**
 * Checks if a gym has been deleted or removed from the database.
 */
async function isGymRemovedFromDb(gymId) {
  if (!gymId) return true;
  try {
    const { data, error } = await supabase
      .from('gyms')
      .select('id')
      .eq('id', gymId)
      .maybeSingle();

    if (error) {
      console.warn(`[Worker] DB check error for gym ${gymId}:`, error.message);
      return false;
    }
    return !data; // true if gym record does not exist in DB
  } catch (err) {
    console.error(`[Worker] Exception in isGymRemovedFromDb for ${gymId}:`, err.message);
    return false;
  }
}

/**
 * Singleton Mutex for Baileys socket creation.
 * Ensures ONLY ONE socket instance connects per gymId at any given time.
 */
async function createBaileysSocket(gymId) {
  // 1. If connection attempt is already in progress, await the existing promise (Mutex / Singleton)
  if (connectingPromises.has(gymId)) {
    console.log(`[Worker] ⏳ Connection attempt already in progress for gym ${gymId}. Awaiting existing promise...`);
    return await connectingPromises.get(gymId);
  }

  // 2. Clear any pending reconnect timers for this gym
  if (reconnectTimers.has(gymId)) {
    clearTimeout(reconnectTimers.get(gymId));
    reconnectTimers.delete(gymId);
  }

  // 3. Return existing connected socket if WebSocket is open
  const existing = activeSockets.get(gymId);
  const isExistingWsOpen = isSocketWsOpen(existing?.socket);
  const hasExistingAuth = Boolean(existing?.socket?.user?.id || existing?.socket?.authState?.creds?.me?.id);
  if (existing?.socket && isExistingWsOpen) {
    if (hasExistingAuth) {
      existing.status = 'connected';
    }
    return existing.socket;
  }

  // 4. Do NOT attempt connection if gym has been removed from DB
  const isRemoved = await isGymRemovedFromDb(gymId);
  if (isRemoved) {
    console.warn(`[Worker] 🚫 Gym ${gymId} is removed from database. Skipping socket creation and cleaning up.`);
    activeSockets.delete(gymId);
    reconnectAttempts.delete(gymId);
    try {
      await supabase.from('whatsapp_sessions').delete().eq('gym_id', gymId);
    } catch {}
    return null;
  }

  // 5. Wrap connection in a singleton promise and register in connectingPromises
  const connectPromise = (async () => {
    console.log(`[Worker] 🔌 Starting WhatsApp socket for gym ${gymId}...`);
    try {
      // Close lingering previous socket if present
      if (existing?.socket) {
        try {
          existing.socket.end(undefined);
        } catch {}
      }

      const { state, saveCreds } = await getWorkerAuthState(gymId);

      const socket = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: 'silent' }),
        browser: ['Gym System Worker', 'Chrome', '1.0.0'],
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 60000,
      });

      ensureSocketReadyState(socket);

      activeSockets.set(gymId, {
        socket,
        status: 'connecting',
        lastSeen: new Date(),
      });

      socket.ev.on('creds.update', async () => {
        try {
          await saveCreds();
        } catch (credErr) {
          console.error(`[Worker] Error in creds.update for gym ${gymId}:`, credErr.message);
        }
      });

      socket.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        const current = activeSockets.get(gymId);

        if (connection === 'open') {
          if (current) current.status = 'connected';
          reconnectAttempts.delete(gymId);
          console.log(`[Worker] ✅ Gym ${gymId} WhatsApp session CONNECTED.`);
        } else if (connection === 'close') {
          const statusCode = lastDisconnect?.error instanceof Boom
            ? lastDisconnect.error.output?.statusCode
            : lastDisconnect?.error?.output?.statusCode;
          const isLoggedOut = statusCode === DisconnectReason.loggedOut || statusCode === 401;
          const isConflict = statusCode === 440 || statusCode === DisconnectReason.connectionReplaced;

          console.log(`[Worker] ⚠️ Socket closed for gym ${gymId}. Code: ${statusCode}, LoggedOut: ${isLoggedOut}, Conflict: ${isConflict}`);

          // Clear any existing reconnect timer for this gym
          if (reconnectTimers.has(gymId)) {
            clearTimeout(reconnectTimers.get(gymId));
            reconnectTimers.delete(gymId);
          }

          // Check if gym is removed from DB
          const isGymDeleted = await isGymRemovedFromDb(gymId);

          // 1. Status 401 (Logged Out) or Gym Removed from DB:
          // Completely purge session credentials and DO NOT attempt reconnection
          if (isLoggedOut || isGymDeleted) {
            console.log(`[Worker] 🔴 Purging credentials for gym ${gymId} (Code: ${statusCode}, GymDeleted: ${isGymDeleted}). Halting reconnection.`);
            activeSockets.delete(gymId);
            reconnectAttempts.delete(gymId);
            connectingPromises.delete(gymId);
            try {
              socket.end(undefined);
            } catch {}
            try {
              await supabase.from('whatsapp_sessions').delete().eq('gym_id', gymId);
            } catch (dbErr) {
              console.error(`[Worker] Error removing session credentials for gym ${gymId}:`, dbErr.message);
            }
            return;
          }

          // 2. Status 440 (Conflict / Invalid Session):
          // Delay reconnection by at least 5 seconds to allow remote socket to close cleanly before retrying
          let delay = 3000;
          if (isConflict) {
            console.warn(`[Worker] ⚠️ Connection conflict (440) for gym ${gymId}. Waiting at least 5s for remote socket to close cleanly before retry...`);
            delay = 5000;
          }

          // 3. Transient disconnects: Enforce Max Reconnect Attempts Limit
          const attempts = (reconnectAttempts.get(gymId) || 0) + 1;
          reconnectAttempts.set(gymId, attempts);

          if (attempts > MAX_RECONNECT_ATTEMPTS) {
            console.error(`[Worker] ❌ Max reconnect attempts (${MAX_RECONNECT_ATTEMPTS}) reached for gym ${gymId}. Halting reconnection.`);
            activeSockets.delete(gymId);
            connectingPromises.delete(gymId);
            return;
          }

          if (current) current.status = 'connecting';
          const finalDelay = Math.max(delay, Math.min(attempts * 2000, 10000));
          console.log(`[Worker] 🔄 Reconnecting gym ${gymId} (attempt ${attempts}/${MAX_RECONNECT_ATTEMPTS}) in ${finalDelay / 1000}s...`);

          const timer = setTimeout(() => {
            reconnectTimers.delete(gymId);
            createBaileysSocket(gymId).catch((e) => console.error(`[Worker] Reconnect error for ${gymId}:`, e?.message || e));
          }, finalDelay);
          reconnectTimers.set(gymId, timer);
        }
      });

      return socket;
    } catch (err) {
      console.error(`[Worker] Failed to create Baileys socket for gym ${gymId}:`, err?.message || err);
      return null;
    } finally {
      connectingPromises.delete(gymId);
    }
  })();

  connectingPromises.set(gymId, connectPromise);
  return await connectPromise;
}

// Alias startGymSocket to createBaileysSocket for backwards-compatibility
const startGymSocket = createBaileysSocket;

// ── 4. Initial Sync of All Active Gym Sessions ─────────────────────────────
async function syncAllGymSessions() {
  try {
    const { data: sessions, error } = await supabase
      .from('whatsapp_sessions')
      .select('gym_id');

    if (error) {
      console.error('[Worker] Error fetching gym sessions from DB:', error);
      return;
    }

    const currentGymIds = new Set((sessions || []).map((s) => s.gym_id));

    // Start sockets for registered gyms (if under max attempts limit and not removed)
    for (const gymId of currentGymIds) {
      const isRemoved = await isGymRemovedFromDb(gymId);
      if (isRemoved) {
        console.warn(`[Worker] Gym ${gymId} exists in sessions but is deleted from gyms table. Purging session.`);
        activeSockets.delete(gymId);
        reconnectAttempts.delete(gymId);
        connectingPromises.delete(gymId);
        try {
          await supabase.from('whatsapp_sessions').delete().eq('gym_id', gymId);
        } catch {}
        continue;
      }

      const attempts = reconnectAttempts.get(gymId) || 0;
      const existing = activeSockets.get(gymId);
      const isConnected = isSocketWsOpen(existing?.socket) && Boolean(existing?.socket?.user?.id || existing?.socket?.authState?.creds?.me?.id);
      if (existing && isConnected) {
        existing.status = 'connected';
      }
      const isConnecting = connectingPromises.has(gymId);

      if (!isConnected && !isConnecting && attempts < MAX_RECONNECT_ATTEMPTS) {
        await createBaileysSocket(gymId);
      }
    }

    // Stop sockets for gyms that were deleted from DB
    for (const [gymId, entry] of activeSockets.entries()) {
      if (!currentGymIds.has(gymId)) {
        console.log(`[Worker] Gym ${gymId} removed from DB. Closing socket.`);
        try {
          entry.socket.end(undefined);
        } catch {}
        activeSockets.delete(gymId);
        reconnectAttempts.delete(gymId);
        connectingPromises.delete(gymId);
      }
    }

    console.log(`[Worker] Sessions synced. Active sockets: ${activeSockets.size}`);
  } catch (err) {
    console.error('[Worker] Error in syncAllGymSessions:', err);
  }
}

// ── 5. Helper to Send Messages via Worker Socket ───────────────────────────
function formatPhone(phone) {
  if (!phone) return '';
  // 1. Strip all non-digit characters (+, spaces, hyphens, etc.)
  let cleaned = String(phone).replace(/\D/g, '');

  // 2. Remove leading double zeros if present (e.g. 002010... -> 2010...)
  cleaned = cleaned.replace(/^00+/, '');

  // 3. Normalize Egyptian numbers:
  // - 11 digits starting with 01 (e.g. 01015629729) -> 201015629729
  if (cleaned.length === 11 && cleaned.startsWith('01')) {
    cleaned = '20' + cleaned.substring(1);
  }
  // - 13 digits starting with 2001 (redundant zero after country code) -> 201...
  else if (cleaned.length === 13 && cleaned.startsWith('2001')) {
    cleaned = '20' + cleaned.substring(3);
  }
  // - 10 digits starting with 10/11/12/15 (without leading 0 or country code) -> 201...
  else if (cleaned.length === 10 && /^(10|11|12|15)/.test(cleaned)) {
    cleaned = '20' + cleaned;
  }

  // 4. Strip any other stray leading zeros (e.g. 020... -> 20...)
  cleaned = cleaned.replace(/^0+/, '');

  return cleaned ? `${cleaned}@s.whatsapp.net` : '';
}

/**
 * Wraps socket.sendMessage in a Promise.race with a strict timeout safeguard
 * so HTTP dispatch calls never hang or block the event loop.
 */
function sendMessageWithTimeout(socket, jid, payload, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    let settled = false;

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        const seconds = Math.round(timeoutMs / 1000);
        reject(new Error(`انتهت مهلة إرسال الرسالة عبر واتساب (${seconds} ثانية)`));
      }
    }, timeoutMs);

    try {
      const sendPromise = socket.sendMessage(jid, payload);
      if (!sendPromise || typeof sendPromise.then !== 'function') {
        settled = true;
        clearTimeout(timer);
        return resolve(sendPromise);
      }

      sendPromise
        .then((res) => {
          if (!settled) {
            settled = true;
            clearTimeout(timer);
            resolve(res);
          }
        })
        .catch((err) => {
          if (!settled) {
            settled = true;
            clearTimeout(timer);
            reject(err);
          }
        });
    } catch (syncErr) {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(syncErr);
      }
    }
  });
}

/**
 * Processes the FIFO queue for a given gym to ensure sequential dispatching
 * and enforce a mandatory throttle delay to prevent Bad MAC session corruption.
 */
async function processGymQueue(gymId) {
  if (isProcessingQueue.get(gymId)) return;
  isProcessingQueue.set(gymId, true);

  try {
    const queue = messageQueues.get(gymId) || [];
    while (queue.length > 0) {
      const job = queue[0];
      
      try {
        const timeout = job.timeoutMs || 15000;
        const result = await sendMessageWithTimeout(job.socket, job.jid, job.payload, timeout);
        job.resolve(result);
        queue.shift(); // Remove on success
      } catch (err) {
        const errMsg = err.message || '';
        const isBadMac = errMsg.includes('Bad MAC') || errMsg.includes('decrypt') || errMsg.includes('Session error');
        
        if (isBadMac) {
          console.error(`[Worker] 🔴 Bad MAC detected for gym ${gymId}. Force closing socket and resetting session...`);
          activeSockets.delete(gymId);
          reconnectAttempts.delete(gymId);
          try {
            job.socket.end(undefined);
            await supabase.from('whatsapp_sessions').delete().eq('gym_id', gymId);
          } catch(e) {}
          
          job.reject(new Error('تمت إعادة ضبط جلسة واتساب بسبب خطأ في التشفير (Bad MAC). يرجى المحاولة مرة أخرى لاحقاً.'));
          queue.shift();
          
          // Clear the rest of the queue since the socket is dead
          while(queue.length > 0) {
            queue.shift().reject(new Error('تم إلغاء الإرسال: جاري إعادة تهيئة جلسة واتساب.'));
          }
          break;
        } else {
          // Normal timeout or other error
          job.reject(err);
          queue.shift();
        }
      }

      // Mandatory throttle delay (1500ms) between messages
      if (queue.length > 0) {
        await new Promise(r => setTimeout(r, 1500));
      }
    }
  } finally {
    isProcessingQueue.set(gymId, false);
  }
}

/**
 * Enqueues a message payload to be sent strictly sequentially per gym.
 */
function enqueueMessage(gymId, socket, jid, payload, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    if (!messageQueues.has(gymId)) {
      messageQueues.set(gymId, []);
    }
    const queue = messageQueues.get(gymId);
    queue.push({ socket, jid, payload, timeoutMs, resolve, reject });
    
    // Kick off queue processing if not running
    processGymQueue(gymId).catch(err => console.error(`[Worker] Queue processor error for gym ${gymId}:`, err));
  });
}

/**
 * Core send message function for the WhatsApp background worker.
 * 
 * Requirements:
 * 1. Check socket readiness instead of string state:
 *    Instead of checking status === 'connected', verify if the Baileys socket exists
 *    and its WebSocket is open (socket?.ws?.readyState === 1).
 * 2. Auto-repair status state:
 *    If socket?.user exists and socket?.ws?.readyState === 1, automatically set
 *    the in-memory gym status to 'connected'.
 * 3. Process the message immediately without throwing the 503 syncing error as long
 *    as the socket is open.
 */
async function sendWorkerMessage(gymId, recipientPhone, content, options = {}) {
  let targetGymId = gymId;
  let text = '';
  let image = null;
  let imageBase64 = null;
  let caption = '';

  if (typeof content === 'string') {
    text = content;
    image = options.image;
    imageBase64 = options.imageBase64;
    caption = options.caption || '';
  } else if (content && typeof content === 'object') {
    text = content.text || '';
    image = content.image || options.image;
    imageBase64 = content.imageBase64 || options.imageBase64;
    caption = content.caption || options.caption || '';
  }

  if (!targetGymId) {
    const err = new Error('معرف النادي (gymId) مفقود في الطلب.');
    err.statusCode = 400;
    throw err;
  }

  if (!recipientPhone) {
    const err = new Error('رقم هاتف المستلم مفقود في الطلب.');
    err.statusCode = 400;
    throw err;
  }

  // 1. Phone number sanitization and JID formatting
  const jid = formatPhone(recipientPhone);
  if (!jid) {
    const err = new Error('رقم الهاتف غير صالح');
    err.statusCode = 400;
    throw err;
  }

  // 2. Ensure gym socket is active and authenticated
  let entry = activeSockets.get(targetGymId);

  // Check if gymId is a slug or different format
  if (!entry) {
    try {
      const { data: gymRow } = await supabase
        .from('gyms')
        .select('id')
        .or(`slug.eq.${targetGymId},id.eq.${targetGymId}`)
        .maybeSingle();
      if (gymRow?.id && activeSockets.has(gymRow.id)) {
        console.log(`[Worker] Resolved gym slug "${targetGymId}" to UUID "${gymRow.id}"`);
        targetGymId = gymRow.id;
        entry = activeSockets.get(targetGymId);
      }
    } catch (e) {}
  }

  // Fallback lookup: If !entry and activeSockets.size === 1, automatically fallback to the single active socket key
  if (!entry && activeSockets.size === 1) {
    const singleKey = Array.from(activeSockets.keys())[0];
    console.log(`[Worker] Falling back to single active socket key: ${singleKey}`);
    targetGymId = singleKey;
    entry = activeSockets.get(singleKey);
  }

  // Auto-repair status state if already open
  if (entry?.socket) ensureSocketReadyState(entry.socket);
  const entryWsOpen = Boolean(entry?.socket?.ws?.readyState === 1 || isSocketWsOpen(entry?.socket));
  const entryHasUser = Boolean(entry?.socket?.user || entry?.socket?.authState?.creds?.me?.id);
  if (entry && entryWsOpen && entryHasUser) {
    entry.status = 'connected';
  }

  // Additional fallback: If entry is missing or not open, find any connected and authenticated socket
  if (!entry || !entryWsOpen) {
    const connectedKey = Array.from(activeSockets.keys()).find(k => {
      const e = activeSockets.get(k);
      if (e?.socket) ensureSocketReadyState(e.socket);
      return Boolean(e?.socket?.ws?.readyState === 1 || isSocketWsOpen(e?.socket)) && 
             Boolean(e?.socket?.user || e?.socket?.authState?.creds?.me?.id);
    });
    if (connectedKey) {
      console.log(`[Worker] Falling back to connected socket key: ${connectedKey}`);
      targetGymId = connectedKey;
      entry = activeSockets.get(connectedKey);
      if (entry) entry.status = 'connected';
    }
  }

  if (entry?.socket) ensureSocketReadyState(entry.socket);

  // [Requirement 1] Check socket readiness instead of string state:
  // Instead of checking status === 'connected', verify if the Baileys socket exists and its WebSocket is open (socket?.ws?.readyState === 1)
  const isWsOpen = Boolean(entry?.socket?.ws?.readyState === 1 || isSocketWsOpen(entry?.socket));
  const hasUser = Boolean(entry?.socket?.user || entry?.socket?.authState?.creds?.me?.id);

  // [Requirement 2] Auto-repair status state:
  // If socket?.user exists and socket?.ws?.readyState === 1, automatically set the in-memory gym status to 'connected'
  if (entry && isWsOpen && hasUser) {
    entry.status = 'connected';
    console.log(`[Worker] Socket is READY & OPEN (readyState === 1) for gym ${targetGymId}. Processing message immediately.`);
  } else {
    // Only if WebSocket is NOT open, check connectingPromises or initiate connection
    if (connectingPromises.has(targetGymId)) {
      console.log(`[Worker] Socket connection in progress for gym ${targetGymId}. Awaiting up to 4s...`);
      try {
        await Promise.race([
          connectingPromises.get(targetGymId),
          new Promise((r) => setTimeout(r, 4000)),
        ]);
      } catch {}
      entry = activeSockets.get(targetGymId);
    } else if (!entry?.socket || !isSocketWsOpen(entry?.socket)) {
      console.log(`[Worker] Socket not connected. Initiating singleton connection for gym ${targetGymId}...`);
      try {
        await Promise.race([
          createBaileysSocket(targetGymId),
          new Promise((r) => setTimeout(r, 4000)),
        ]);
      } catch {}
      entry = activeSockets.get(targetGymId);
    }
  }

  // Re-verify socket readiness and auto-repair status state
  if (entry?.socket) ensureSocketReadyState(entry.socket);
  const finalWsOpen = Boolean(entry?.socket?.ws?.readyState === 1 || isSocketWsOpen(entry?.socket));
  const finalHasUser = Boolean(entry?.socket?.user || entry?.socket?.authState?.creds?.me?.id);

  if (entry && finalWsOpen && finalHasUser) {
    entry.status = 'connected';
  }

  const isSocketReady = Boolean(entry?.socket && finalWsOpen && finalHasUser);

  if (!isSocketReady) {
    const wsState = getSocketReadyState(entry?.socket);
    const isConnecting = (wsState === 0) || connectingPromises.has(targetGymId) || (entry?.status === 'connecting' && !finalWsOpen);
    console.warn(`[CRITICAL_MESSAGES_ERROR] sendWorkerMessage rejected (503): WhatsApp socket not ready for gym ${targetGymId}. Status: ${entry?.status}, WS readyState: ${wsState}, hasUser: ${finalHasUser}`);

    const errorMessage = isConnecting
      ? 'خادم واتساب قيد الاتصال حالياً (جاري المزامنة). يرجى الانتظار بضع ثوانٍ وإعادة المحاولة.'
      : 'خدمة واتساب غير متصلة أو لم يتم ربطها لهذا النادي بعد (503 Service Unavailable). يرجى التحقق من الربط في الإعدادات.';

    const err = new Error(errorMessage);
    err.statusCode = 503;
    err.status = isConnecting ? 'connecting' : 'disconnected';
    err.wsReadyState = wsState;
    throw err;
  }

  // [Requirement 3] Process the message immediately without throwing the 503 syncing error as long as the socket is open
  let targetJid = jid;
  try {
    if (typeof entry.socket.onWhatsApp === 'function') {
      const checkPromise = entry.socket.onWhatsApp(jid);
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('onWhatsApp check timed out')), 5000)
      );
      const [result] = await Promise.race([checkPromise, timeoutPromise]);
      if (result && !result.exists) {
        const notFoundErr = new Error('هذا الرقم غير مسجل على واتساب');
        notFoundErr.statusCode = 400;
        throw notFoundErr;
      }
      if (result?.jid) {
        targetJid = result.jid;
      }
    }
  } catch (checkErr) {
    if (checkErr.statusCode === 400) throw checkErr;
    console.warn(`[Worker] onWhatsApp check warning for ${jid}:`, checkErr.message);
  }

  // Send message or media with sequential queue integration (15s timeout safeguard)
  const mediaImage = image || imageBase64;
  let result;
  if (mediaImage) {
    const mimeMatch = typeof mediaImage === 'string' ? mediaImage.match(/^data:(image\/\w+);base64,/) : null;
    const mimetype = mimeMatch ? mimeMatch[1] : 'image/jpeg';
    const cleanBase64 = typeof mediaImage === 'string' ? mediaImage.replace(/^data:image\/\w+;base64,/, '') : mediaImage;
    const buffer = Buffer.isBuffer(cleanBase64) ? cleanBase64 : Buffer.from(cleanBase64, 'base64');

    result = await enqueueMessage(targetGymId, entry.socket, targetJid, { image: buffer, caption: caption || text, mimetype }, 15000);
  } else {
    result = await enqueueMessage(targetGymId, entry.socket, targetJid, { text }, 15000);
  }

  return {
    success: true,
    messageId: result?.key?.id || undefined,
  };
}

const sendMessage = sendWorkerMessage;

// ── 6. Lightweight HTTP Health Check & Control Server ──────────────────────
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  // Express-like compatibility helpers
  res.status = (code) => {
    res.statusCode = code;
    return res;
  };
  res.json = (data) => {
    if (!res.headersSent) {
      res.setHeader('Content-Type', 'application/json');
    }
    return res.end(JSON.stringify(data));
  };

  // Set standard CORS headers for cross-origin frontend browser requests
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  // Handle preflight OPTIONS requests
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  // Health check endpoint for Render / Railway / Uptime monitoring
  if (req.method === 'GET' && (url.pathname === '/health' || url.pathname === '/')) {
    return res.status(200).json({
      status: 'ok',
      uptime: Math.floor(process.uptime()),
      activeGymSessions: activeSockets.size,
      connectedGyms: Array.from(activeSockets.entries()).map(([gymId, s]) => ({
        gymId,
        status: s.status,
      })),
      timestamp: new Date().toISOString(),
    });
  }

  // Throttled gym status endpoint for frontend polling
  if (req.method === 'GET' && url.pathname === '/api/status') {
    const gymId = url.searchParams.get('gymId');
    if (!gymId) {
      return res.status(400).json({ success: false, error: 'Missing gymId query parameter' });
    }

    const now = Date.now();
    const cached = statusCache.get(gymId);
    if (cached && now - cached.timestamp < 2000) {
      return res.status(200).json(cached.data);
    }

    let entry = activeSockets.get(gymId);
    if (!entry && activeSockets.size === 1) {
      entry = Array.from(activeSockets.values())[0];
    }

    if (entry?.socket) ensureSocketReadyState(entry.socket);
    const isWsOpen = Boolean(entry?.socket?.ws?.readyState === 1 || isSocketWsOpen(entry?.socket));
    const hasAuthUser = Boolean(entry?.socket?.user?.id || entry?.socket?.authState?.creds?.me?.id);

    // Auto-repair status state: If socket?.user exists and readyState === 1, automatically update in-memory gym status to 'connected'
    if (entry && isWsOpen && hasAuthUser) {
      entry.status = 'connected';
    }

    const isConnected = Boolean(isWsOpen && (hasAuthUser || entry?.status === 'connected'));
    let phoneNumber = null;
    if (entry?.socket?.user?.id) {
      phoneNumber = entry.socket.user.id.split(':')[0]?.split('@')[0] || null;
    } else if (entry?.socket?.authState?.creds?.me?.id) {
      phoneNumber = entry.socket.authState.creds.me.id.split(':')[0]?.split('@')[0] || null;
    }

    const responseData = {
      success: true,
      gymId,
      status: isConnected ? 'connected' : (entry?.status || 'disconnected'),
      isReady: isConnected,
      phoneNumber,
      wsReadyState: getSocketReadyState(entry?.socket),
      timestamp: new Date().toISOString(),
    };

    statusCache.set(gymId, { data: responseData, timestamp: now });
    return res.status(200).json(responseData);
  }

  // Force session sync
  if (req.method === 'POST' && url.pathname === '/api/sync') {
    reconnectAttempts.clear();
    await syncAllGymSessions();
    return res.status(200).json({ success: true, activeSessions: activeSockets.size });
  }

  // Send message endpoint
  if (req.method === 'POST' && url.pathname === '/api/send') {
    let body = '';
    let tooLarge = false;

    req.on('error', (err) => {
      console.error('[Worker] /api/send stream error:', err.message);
      body = '';
      if (!res.headersSent) {
        res.status(500).json({ success: false, error: err.message });
      }
    });

    req.on('data', (chunk) => {
      if (tooLarge) return;
      body += chunk;
      if (body.length > 15 * 1024 * 1024) {
        tooLarge = true;
        body = '';
        res.status(413).json({ success: false, error: 'Payload too large (max 15MB)' });
        req.destroy();
      }
    });

    req.on('end', async () => {
      if (tooLarge) return;
      try {
        let parsed = null;
        try {
          parsed = JSON.parse(body);
        } catch (jsonErr) {
          console.error('[CRITICAL_MESSAGES_ERROR] JSON parse error in /api/send:', jsonErr.message);
          return res.status(400).json({ success: false, error: 'تنسيق البيانات غير صالح (Invalid JSON payload)' });
        }
        body = ''; // Free memory immediately

        const gymId = parsed?.gymId;
        const phone = parsed?.phone;
        const text = parsed?.text || parsed?.message || '';
        const memberId = parsed?.memberId;
        const imageBase64 = parsed?.imageBase64;
        const image = parsed?.image;
        const caption = parsed?.caption;

        if (!gymId) {
          return res.status(400).json({ success: false, error: 'معرف النادي (gymId) مفقود في الطلب.' });
        }
        if (!phone) {
          return res.status(400).json({ success: false, error: 'رقم هاتف المستلم مفقود في الطلب.' });
        }

        const result = await sendWorkerMessage(gymId, phone, text, {
          memberId,
          image,
          imageBase64,
          caption,
        });

        return res.status(200).json(result);
      } catch (err) {
        console.error('[CRITICAL_MESSAGES_ERROR] Worker exception in /api/send:', err.stack || err);
        const statusCode = err.statusCode || (err.message?.includes('مهلة') || err.message?.includes('timeout') ? 408 : 500);
        return res.status(statusCode).json({
          success: false,
          error: err.message || 'فشل إرسال رسالة واتساب عبر الخادم',
          status: err.status,
          wsReadyState: err.wsReadyState,
        });
      }
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not Found' }));
});

// ── 7. Bootstrap & Lifecycle ───────────────────────────────────────────────
if (require.main === module) {
  server.listen(PORT, async () => {
    console.log(`=======================================================`);
    console.log(`🚀 Taqa WhatsApp Background Worker started on port ${PORT}`);
    console.log(`📡 Supabase Endpoint: ${SUPABASE_URL}`);
    console.log(`=======================================================`);

    // Initial sync
    await syncAllGymSessions();

    // Periodic resync every 60 seconds to detect new gym pairings
    setInterval(syncAllGymSessions, 60000);
  });
}

// Graceful shutdown
function shutdown(signal) {
  console.log(`\n[Worker] Received ${signal}. Gracefully closing WhatsApp sockets...`);
  for (const [gymId, timer] of reconnectTimers.entries()) {
    clearTimeout(timer);
  }
  reconnectTimers.clear();
  connectingPromises.clear();

  for (const [gymId, entry] of activeSockets.entries()) {
    try {
      entry.socket.end(undefined);
    } catch {}
  }
  server.close(() => {
    console.log('[Worker] Server closed. Exiting process.');
    process.exit(0);
  });
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// ── 8. Global Decryption/Crypto Error Suppressors (Prevents Process Crashes) ──
process.on('unhandledRejection', (reason) => {
  const reasonStr = String(reason?.stack || reason?.message || reason);
  if (
    reasonStr.includes('Bad MAC') ||
    reasonStr.includes('decrypt') ||
    reasonStr.includes('Session error') ||
    reasonStr.includes('SessionEntry') ||
    reasonStr.includes('conflict') ||
    reasonStr.includes('Connection Closed')
  ) {
    console.warn('[Worker] ⚠️ Suppressed unhandled session/decryption error to prevent process crash:', reason?.message || reason);
    return;
  }
  console.error('[Worker] Unhandled Rejection:', reason);
});

process.on('uncaughtException', (err) => {
  const errStr = String(err?.stack || err?.message || err);
  if (
    errStr.includes('Bad MAC') ||
    errStr.includes('decrypt') ||
    errStr.includes('Session error') ||
    errStr.includes('SessionEntry') ||
    errStr.includes('conflict') ||
    errStr.includes('Connection Closed')
  ) {
    console.warn('[Worker] ⚠️ Suppressed uncaught session/decryption error to prevent process crash:', err?.message || err);
    return;
  }
  console.error('[Worker] Uncaught Exception:', err);
});

module.exports = {
  sendWorkerMessage,
  sendMessage,
  isSocketWsOpen,
  getSocketReadyState,
  syncAllGymSessions,
  activeSockets,
  server,
};
