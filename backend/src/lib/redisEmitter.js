import { Emitter } from "@socket.io/redis-emitter";
import { pubClient } from "./redis.js";

// The Kafka worker is a plain Node process — it never runs a Socket.IO
// server and never holds any socket connections. This emitter lets it
// publish "emit this event to room X" messages over the same Redis pub/sub
// channel that every API instance's socket.io redis adapter is subscribed
// to, so the message still reaches the right connected client no matter
// which API replica they're attached to.
export const emitter = new Emitter(pubClient);
