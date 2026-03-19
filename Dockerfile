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

# --expose-gc enables global.gc() for manual garbage collection between sources
# --max-old-space-size=400 caps V8 heap to stay within Railway's 512MB limit
CMD ["node", "--expose-gc", "--max-old-space-size=400", "dist/index.js"]
