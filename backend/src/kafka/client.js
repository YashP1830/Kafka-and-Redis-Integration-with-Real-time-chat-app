import { Kafka, logLevel } from "kafkajs";

// KAFKA_BROKERS supports a comma separated list, e.g. "kafka1:9092,kafka2:9092"
const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((b) => b.trim())
  .filter(Boolean);

export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "realtime-chat-app",
  brokers,
  retry: {
    initialRetryTime: 300,
    retries: 8,
  },
  logLevel: logLevel.NOTHING, // kafkajs is noisy by default, we log ourselves
});
