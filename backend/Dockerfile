# syntax=docker/dockerfile:1

FROM node:20-alpine AS base
WORKDIR /app
ENV NODE_ENV=production

# Install deps first (better layer caching)
COPY package*.json ./
RUN npm ci --omit=dev

# Copy source
COPY src ./src

# Run as a non-root user
RUN addgroup -S nodejs && adduser -S nodeuser -G nodejs
USER nodeuser

EXPOSE 3000

# Default command runs the HTTP/Socket.IO API.
# The worker service overrides this in docker-compose.yml with:
#   command: ["node", "src/worker.js"]
CMD ["node", "src/server.js"]
