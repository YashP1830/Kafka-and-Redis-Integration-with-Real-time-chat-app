import http from "http";
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import { socketAuthMiddleware } from "../middleware/socketAuthMiddleware.js";
import { pubClient, subClient, redis } from "./redis.js";

let io;

const ONLINE_USERS_KEY = "chat:online-users"; // redis hash: userId -> connection count

export const initSocket = (app) => {
  const server = http.createServer(app);

  io = new Server(server, {
    cors: {
      origin: process.env.CLIENT_URL,
      credentials: true,
    },
  });

  // ✅ Redis adapter: makes io.emit / io.to(room).emit work across every
  // horizontally-scaled instance of this API, not just the process that
  // owns the socket. This is what lets us run N replicas behind a load
  // balancer while still broadcasting presence/messages to everyone.
  io.adapter(createAdapter(pubClient, subClient));

  io.use(socketAuthMiddleware);

  io.on("connection", async (socket) => {
    const userId = socket.userId;
    console.log(`🟢 Socket connected: ${socket.id} (user ${socket.user.fullName})`);

    // Each user gets a room named after their own id. Any process (API
    // instance or the Kafka worker via the redis-emitter) can then deliver
    // a message with io/emitter.to(userId).emit(...) without needing to know
    // which physical instance/socketId the user is attached to.
    socket.join(userId);

    // A user can have multiple tabs/devices open, so track a connection
    // count per user in Redis rather than a boolean, and only flip them
    // "offline" once their last connection drops.
    const connections = await redis.hincrby(ONLINE_USERS_KEY, userId, 1);
    if (connections === 1) {
      await broadcastOnlineUsers();
    }

    socket.on("disconnect", async () => {
      console.log(`🔴 Socket disconnected: ${socket.id}`);
      const remaining = await redis.hincrby(ONLINE_USERS_KEY, userId, -1);
      if (remaining <= 0) {
        await redis.hdel(ONLINE_USERS_KEY, userId);
        await broadcastOnlineUsers();
      }
    });
  });

  return server;
};

async function broadcastOnlineUsers() {
  const onlineUserIds = await redis.hkeys(ONLINE_USERS_KEY);
  io.emit("getOnlineUsers", onlineUserIds);
}

export { io };
