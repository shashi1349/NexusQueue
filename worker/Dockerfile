# Stage 1: Build
FROM node:22-alpine AS builder

WORKDIR /app

# Copy root package files and workspace package.json files
COPY package.json package-lock.json ./
COPY shared/package.json shared/
COPY server/package.json server/
COPY worker/package.json worker/
COPY dashboard/package.json dashboard/

# Install all dependencies (workspaces need all package.json files present)
RUN npm ci

# Copy source for shared and worker
COPY shared/ shared/
COPY worker/ worker/

# Build shared first, then worker
RUN npm run build --workspace @nexusqueue/shared && npm run build --workspace @nexusqueue/worker

# Stage 2: Runtime
FROM node:22-alpine

WORKDIR /app

# Copy package files for production install
COPY package.json package-lock.json ./
COPY shared/package.json shared/
COPY server/package.json server/
COPY worker/package.json worker/
COPY dashboard/package.json dashboard/

# Install production dependencies only
RUN npm ci --omit=dev

# Copy built output from builder
COPY --from=builder /app/shared/dist/ shared/dist/
COPY --from=builder /app/worker/dist/ worker/dist/

ENV WORKER_QUEUE=default \
    WORKER_CONCURRENCY=5 \
    WORKER_ID=worker-1

CMD ["node", "worker/dist/index.js"]
