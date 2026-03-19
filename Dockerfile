FROM node:20-slim

WORKDIR /app

# Install all deps (including typescript for build)
COPY package*.json ./
RUN npm ci

# Copy source and compile
COPY tsconfig.json ./
COPY src/ ./src/
COPY .well-known/ ./.well-known/
COPY mcp/ ./mcp/
RUN npx tsc

# Remove dev deps for smaller image
RUN npm prune --omit=dev

EXPOSE 3000

CMD ["node", "dist/index.js"]
