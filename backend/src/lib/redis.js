import Redis from "ioredis";

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

// Two dedicated connections are required by the socket.io redis adapter
// (one for publishing, one for subscribing) — a single client can't do both.
export const pubClient = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
export const subClient = pubClient.duplicate();

// A general-purpose client for presence tracking (online users set) and
// anything else that isn't strictly pub/sub.
export const redis = pubClient.duplicate();

pubClient.on("error", (err) => console.error("❌ Redis pub client error:", err.message));
subClient.on("error", (err) => console.error("❌ Redis sub client error:", err.message));
redis.on("error", (err) => console.error("❌ Redis client error:", err.message));
