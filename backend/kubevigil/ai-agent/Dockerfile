FROM node:20-alpine
RUN apk add --no-cache curl && \
    curl -LO "https://dl.k8s.io/release/v1.28.0/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && mv kubectl /usr/local/bin/
WORKDIR /app
COPY package*.json ./
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]