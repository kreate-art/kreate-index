FROM --platform=$TARGETPLATFORM node:18.14.0-bullseye-slim AS base
RUN mkdir -p /app && chown -R node:node /app
WORKDIR /app
USER node

FROM base AS builder
COPY --chown=node:node package*.json ./
RUN npm ci
COPY --chown=node:node tsconfig*.json ./
COPY --chown=node:node src src
RUN npm run build && \
    npm prune --omit=dev && \
    npm cache clean --force

########################################

FROM base

USER root
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static /tini
RUN chmod +x /tini

ENV NODE_ENV=production
COPY --from=builder --chown=node:node /app/package*.json ./
COPY --from=builder --chown=node:node /app/node_modules node_modules
COPY --from=builder --chown=node:node /app/dist dist
COPY --chown=node:node config config

ARG COMMIT_SHA
ENV COMMIT_SHA=$COMMIT_SHA
RUN echo "$COMMIT_SHA" > ./__commit_sha__
LABEL commit-sha="$COMMIT_SHA"

USER node

ENTRYPOINT ["/tini", "--"]
CMD ["node", "dist/index.js", "all"]
