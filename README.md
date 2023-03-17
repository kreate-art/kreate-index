# Kreate Index

This repository contains the Kreate Index implementation in Gen I.

## Docker

### Build

```sh
docker buildx build \
  --build-arg "COMMIT_SHA=$(git rev-parse HEAD)" \
  -t kreate/index:latest .
```

### Run

Kreate Index depends on a few other services:

- cardano-node (https://github.com/input-output-hk/cardano-node)
- ogmios (https://github.com/CardanoSolutions/ogmios)
- ipfs (https://github.com/ipfs/kubo)
- A PostgreSQL database

A minimal working setup to get those services up and running is defined in `docker-compose.yml`.

```sh
# Start (kreate-index and required services)
COMMIT_SHA=$(git rev-parse HEAD) docker-compose up -d --build --remove-orphans
# Check kreate-index logs
docker-compose logs -t -f index
# Stop
docker-compose down
```

## Development

You need to configure your own `.env` file. An example of the running protocol on testnet (https://testnet.kreate.community) can be found in `.env.testnet`.

### Start

```sh
npm ci
npm run start all
```
