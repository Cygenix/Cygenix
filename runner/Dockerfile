# Cygenix migration runner — standalone Node script that ports the migration
# logic from netlify/functions/scheduler-run-background.js to a container.
# Triggered by Netlify cron (or run-now) via Azure REST API, runs to
# completion with no time cap, writes results to Cosmos and exits.
#
# Build:    docker build -t cygenixacr.azurecr.io/cygenix-runner:latest .
# Push:     docker push cygenixacr.azurecr.io/cygenix-runner:latest
# Run in ACI: triggered by scheduler.js / scheduled-runner.js via the Azure
# Container Instances REST API.

FROM node:20-alpine

# Install ca-certificates for the mssql driver's TLS validation against
# Azure SQL endpoints, and tini for clean signal handling (so Azure can
# stop the container cleanly if it ever needs to).
RUN apk add --no-cache ca-certificates tini

WORKDIR /app

# Copy package files first so Docker can cache the npm install layer when
# only the source changes — saves rebuild time on the runner code.
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev --no-audit --no-fund || npm install --omit=dev --no-audit --no-fund

# Copy the runner source
COPY index.js ./

# Run as non-root for defence in depth — the runner only reads from Cosmos
# and connects out to SQL Server endpoints, so it has no need for elevated
# rights inside the container.
RUN addgroup -S cygenix && adduser -S cygenix -G cygenix && chown -R cygenix:cygenix /app
USER cygenix

# tini handles SIGTERM properly so Azure can shut the container down
# cleanly on stop / scale-in.
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "index.js"]
