FROM node:24-alpine

WORKDIR /app
COPY worker/worker.mjs /app/worker.mjs
COPY kusto /app/kusto

CMD ["node", "/app/worker.mjs"]
