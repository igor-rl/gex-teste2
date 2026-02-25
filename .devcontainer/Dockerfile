FROM node:20-bookworm-slim

RUN apt-get update && apt-get install -y \
    git curl bash zsh postgresql-client redis-tools make python3 \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g nodemon tsx typescript

WORKDIR /app
CMD ["sleep", "infinity"]