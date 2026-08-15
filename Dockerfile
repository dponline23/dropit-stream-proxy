FROM node:22-alpine

WORKDIR /app

COPY package.json ./
COPY server.mjs ./

ENV NODE_ENV=production

CMD ["node", "server.mjs"]
