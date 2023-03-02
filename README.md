# teiki-index

This repository contains the Teiki index implementation in Generation I.

## Docker

### Build

```sh
docker buildx build \
  --build-arg "COMMIT_SHA=$(git rev-parse HEAD)" \
  -t teiki/index:latest .
```

### Run

Teiki Index depends on a few other services:

- cardano-node (https://github.com/input-output-hk/cardano-node)
- ogmios (https://github.com/CardanoSolutions/ogmios)
- ipfs (https://github.com/ipfs/kubo)
- A PostgreSQL database

A minimal working setup to get those services up and running is defined in `docker-compose.yml`.

```sh
# Start (teiki-index and required services)
COMMIT_SHA=$(git rev-parse HEAD) docker-compose up -d --build --remove-orphans
# Check teiki-index logs
docker-compose logs -t -f index
# Stop
docker-compose down
```

## Development

You need to configure your own `.env` file. An example of the running protocol on testnet (https://testnet.teiki.network) can be found in `.env.testnet`.

### Start

```sh
npm ci
npm run start all
```

---

Feel free to connect: [Website](https://teiki.network), [Medium](https://teikinetwork.medium.com), [Discord](https://discord.gg/n9wZZTY6XA), [Twitter](https://twitter.com/TeikiNetwork), [Telegram](https://t.me/teiki_announcement). We are very open to discussions, questions, and feedback :seedling:.
