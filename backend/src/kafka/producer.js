import { kafka } from "./client.js";

const producer = kafka.producer({
  allowAutoTopicCreation: true,
  idempotent: true, // avoids duplicate messages on broker retries
});

let isConnected = false;

export const connectProducer = async () => {
  if (isConnected) return;
  await producer.connect();
  isConnected = true;
  console.log("📤 Kafka producer connected");
};

export const disconnectProducer = async () => {
  if (!isConnected) return;
  await producer.disconnect();
  isConnected = false;
};

/**
 * Publish an event onto a topic.
 * `key` should be something stable (e.g. conversation id / receiverId) so that
 * Kafka routes all messages for the same conversation to the same partition,
 * which preserves per-conversation ordering even with multiple partitions/consumers.
 */
export const publishEvent = async (topic, key, payload) => {
  if (!isConnected) {
    await connectProducer();
  }

  await producer.send({
    topic,
    messages: [
      {
        key: String(key),
        value: JSON.stringify(payload),
        headers: {
          "content-type": "application/json",
          "produced-at": new Date().toISOString(),
        },
      },
    ],
  });
};

export default producer;
