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

# Copy source for shared and server
COPY shared/ shared/
COPY server/ server/

# Build shared first, then server
RUN npm run build --workspace @nexusqueue/shared && npm run build --workspace @nexusqueue/server

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
COPY --from=builder /app/server/dist/ server/dist/

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

CMD ["node", "server/dist/index.js"]
