import dotenv from "dotenv";
import { connectDB } from "./lib/db.js";
import { startConsumer, stopConsumer } from "./kafka/consumer.js";

dotenv.config();

(async () => {
  await connectDB();
  await startConsumer();
  console.log("🚀 Chat message worker is running");
})().catch((err) => {
  console.error("❌ Worker failed to start:", err);
  process.exit(1);
});

const shutdown = async (signal) => {
  console.log(`\n${signal} received, shutting down worker gracefully...`);
  await stopConsumer();
  process.exit(0);
};

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
