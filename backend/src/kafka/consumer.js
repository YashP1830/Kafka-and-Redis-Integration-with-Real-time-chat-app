import { kafka } from "./client.js";
import { TOPICS } from "./topics.js";
import { publishEvent } from "./producer.js";
import Message from "../models/message.js";
import { emitter } from "../lib/redisEmitter.js";

const GROUP_ID = process.env.KAFKA_CONSUMER_GROUP || "chat-message-workers";
const MAX_RETRIES = 3;

const consumer = kafka.consumer({ groupId: GROUP_ID });

/**
 * Handles a single "chat message created" event:
 *  1. Persist it to MongoDB (source of truth).
 *  2. Deliver it in real time to sender + receiver via the redis emitter,
 *     which reaches whichever API instance actually holds their socket.
 *
 * Running this in a dedicated worker (separate from the HTTP API process)
 * means message persistence/fan-out can be scaled independently — e.g. spin
 * up more worker replicas or more partitions when write volume spikes,
 * without touching the API tier at all.
 */
async function handleChatMessage(payload) {
  const { _id, senderId, receiverId, text, image, createdAt } = payload;

  const message = await Message.create({
    _id,
    senderId,
    receiverId,
    text,
    image,
    createdAt,
  });

  // Deliver to the receiver (main purpose) and back to the sender's other
  // connected devices/tabs so all of the sender's sessions stay in sync.
  emitter.to(receiverId.toString()).emit("newMessage", message);
  emitter.to(senderId.toString()).emit("newMessage", message);

  return message;
}

export const startConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.CHAT_MESSAGES, fromBeginning: false });
  console.log(`📥 Kafka consumer subscribed to "${TOPICS.CHAT_MESSAGES}" (group: ${GROUP_ID})`);

  await consumer.run({
    // Processing one message at a time per partition keeps per-conversation
    // ordering intact (messages are keyed by conversation in the producer).
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let payload;
      try {
        payload = JSON.parse(raw);
      } catch (err) {
        console.error("❌ Skipping unparseable Kafka message:", err.message);
        return;
      }

      const attempt = payload.__attempt || 1;

      try {
        await handleChatMessage(payload);
      } catch (err) {
        console.error(
          `❌ Failed to process message (attempt ${attempt}/${MAX_RETRIES}):`,
          err.message
        );

        if (attempt < MAX_RETRIES) {
          // Re-publish with an incremented attempt counter instead of
          // crashing the consumer loop or blocking the partition.
          await publishEvent(topic, message.key?.toString() || payload.receiverId, {
            ...payload,
            __attempt: attempt + 1,
          });
        } else {
          // Exhausted retries — park it in the dead-letter topic instead of
          // silently dropping the message, so it can be inspected/replayed.
          await publishEvent(TOPICS.CHAT_MESSAGES_DLQ, partition, payload);
          console.error("☠️  Message moved to DLQ after exhausting retries:", payload._id);
        }
      }
    },
  });
};

export const stopConsumer = async () => {
  await consumer.disconnect();
};
