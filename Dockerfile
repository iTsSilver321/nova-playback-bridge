FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates python3 make g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV FFMPEG_BIN=/usr/bin/ffmpeg NOVA_FFMPEG=/usr/bin/ffmpeg NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY server ./server
RUN mkdir /cache && chown node:node /cache
USER node
ENV NOVA_BRIDGE_BIND=0.0.0.0 NOVA_BRIDGE_CACHE=/cache NOVA_BRIDGE_JOBS=1 NOVA_BRIDGE_SESSIONS=2
EXPOSE 10000
CMD ["node","server/bridge/service.mjs"]
