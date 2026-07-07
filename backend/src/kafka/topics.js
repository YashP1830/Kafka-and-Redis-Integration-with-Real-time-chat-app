// Centralized topic names so producers/consumers never drift out of sync.
export const TOPICS = {
  // A new chat message that needs to be persisted + fanned out to the receiver.
  CHAT_MESSAGES: "chat-messages",

  // Dead-letter topic for messages that failed processing after retries.
  CHAT_MESSAGES_DLQ: "chat-messages-dlq",
};
