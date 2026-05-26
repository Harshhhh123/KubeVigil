FROM node:18-alpine

# Create app directory inside the container
WORKDIR /app

# Copy package files first — Docker caches this layer
# So npm install only reruns when package.json changes, not on every code change
COPY package*.json ./
RUN npm install --production

# Copy the actual source code
COPY index.js .

# This service has no HTTP port — it just watches and publishes to Kafka
CMD ["node", "index.js"]