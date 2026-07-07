import express from "express";
import dotenv from "dotenv";
import path from "path";
import cookieParser from "cookie-parser";
import cors from "cors";

import authRoutes from "./routes/auth.route.js";
import messageRoutes from "./routes/message.route.js";
import { connectDB } from "./lib/db.js";
import { initSocket } from "./lib/socket.js";
import { connectProducer } from "./kafka/producer.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;
const __dirname = path.resolve();

// ✅ MIDDLEWARES FIRST
app.use(
  cors({
    origin: process.env.CLIENT_URL || "http://localhost:5173",
    credentials: true,
  })
);

app.use(express.json({ limit: "5mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// ✅ ROUTES
app.use("/api/auth", authRoutes);
app.use("/api/message", messageRoutes);

// ✅ INIT SOCKET **AFTER** MIDDLEWARE
const server = initSocket(app);

// ✅ START SERVER
server.listen(PORT, async () => {
  console.log(`🚀 Server running on port ${PORT}`);
  await connectDB();
  await connectProducer(); // this API instance only produces to Kafka; the
  // worker process (src/worker.js) owns the consumer + DB writes for messages
});

// ✅ PRODUCTION FRONTEND SERVE
if (process.env.NODE_ENV === "production") {
  app.use(express.static(path.join(__dirname, "../frontend/dist")));

  app.get("*", (req, res) => {
    res.sendFile(path.join(__dirname, "../frontend/dist/index.html"));
  });
}
