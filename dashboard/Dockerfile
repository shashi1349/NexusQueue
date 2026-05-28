# Stage 1: Build
FROM node:22-alpine AS builder

WORKDIR /app

# Copy root package files (npm workspaces need the root lock file)
COPY package.json package-lock.json ./
COPY dashboard/package.json dashboard/
COPY shared/package.json shared/
COPY server/package.json server/
COPY worker/package.json worker/

# Install dependencies
RUN npm ci --workspace @nexusqueue/dashboard

# Copy dashboard source
COPY dashboard/ dashboard/

# Build the dashboard (vite build produces dist/)
RUN npm run build --workspace @nexusqueue/dashboard

# Stage 2: Runtime
FROM nginx:alpine

# Copy built static files
COPY --from=builder /app/dashboard/dist/ /usr/share/nginx/html/

# Create nginx config for SPA routing
RUN printf 'server {\n\
    listen 80;\n\
    server_name _;\n\
    root /usr/share/nginx/html;\n\
    index index.html;\n\
\n\
    location / {\n\
        try_files $uri $uri/ /index.html;\n\
    }\n\
}\n' > /etc/nginx/conf.d/default.conf

EXPOSE 80
