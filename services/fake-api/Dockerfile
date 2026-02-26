FROM node:20-alpine

WORKDIR /app

COPY services/fake-api/package*.json ./
RUN npm install

COPY services/fake-api/server.js ./

EXPOSE 8080

CMD ["node", "server.js"]