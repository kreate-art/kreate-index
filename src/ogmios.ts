import { scheduler } from "node:timers/promises";

import {
  ConnectionConfig,
  createInteractionContext,
  InteractionContext,
  InteractionType,
  ServerNotReady,
} from "@cardano-ogmios/client";

import { MaybePromise } from "./types/typelevel";

export type OgmiosContextFactory = {
  contexts: OgmiosContext[];
  create: (
    name: string,
    options?: {
      interactionType?: InteractionType;
    }
  ) => Promise<OgmiosContext>;
  shutdown: (
    fn?: (context: OgmiosContext) => MaybePromise<void>
  ) => Promise<void>;
};

export type OgmiosContext = InteractionContext & {
  name: string;
  shutdown: (fn?: () => MaybePromise<void>) => Promise<void>;
};

export function createOgmiosContextFactory(
  connection: ConnectionConfig,
  fn?: (context: OgmiosContext) => MaybePromise<void>
): OgmiosContextFactory {
  const contexts: OgmiosContext[] = [];
  return {
    contexts,
    create: async function (name, options) {
      // console.log(`<ogmios>{${name}} Config:`, options);
      // const { connection: overrides, ...opts } = options;
      // const merged = { ...connection, ...overrides };
      const context = await createOgmiosContext(connection, name, options);
      contexts.push(context);
      return context;
    },
    shutdown: async function () {
      await Promise.all(
        contexts.map((c) => c.shutdown(fn ? () => fn(c) : undefined))
      );
      contexts.length = 0;
    },
  };
}

export async function createOgmiosContext(
  connection: ConnectionConfig,
  name: string,
  options?: { interactionType?: InteractionType }
): Promise<OgmiosContext> {
  let isShuttingDown = false;

  const tag = `<ogmios>{${name}}`;

  function onClientError(error: Error) {
    console.error(`${tag} Error:`, error);
    throw error;
  }

  function onClientClose(code: number, reason: string) {
    const message = `${tag} Close (${code}) ${reason}`;
    if (isShuttingDown) console.warn(message);
    else throw new Error(message);
  }

  console.log(`${tag} Connecting...`);
  for (;;) {
    try {
      const context = await createInteractionContext(
        onClientError,
        onClientClose,
        { connection, interactionType: options?.interactionType }
      );
      console.log(`${tag} Connected!`);
      return {
        ...context,
        name,
        shutdown: async function (
          this: OgmiosContext,
          fn?: () => MaybePromise<void>
        ): Promise<void> {
          this.shutdown = Promise.resolve.bind(Promise);
          isShuttingDown = true;
          fn && (await fn());
          await new Promise<void>((resolve, reject) => {
            const socket = this.socket;
            switch (socket.readyState) {
              case socket.CONNECTING:
                console.warn(`${tag} just connecting...`);
                socket.once("close", () => {
                  console.log(`${tag} Disconnected!`);
                  resolve();
                });
                socket.once("error", (e) => reject(e));
                return socket.close();
              case socket.OPEN:
                console.log(`${tag} Disconnecting...`);
                socket.once("close", () => {
                  console.log(`${tag} Disconnected!`);
                  resolve();
                });
                socket.once("error", (e) => reject(e));
                return socket.close();
              case socket.CLOSING:
                console.warn(`${tag} already closing...`);
                return resolve();
              case socket.CLOSED:
                console.warn(`${tag} already closed...`);
                return resolve();
            }
          });
        },
      };
    } catch (e) {
      if (e instanceof ServerNotReady) {
        console.warn(`${tag} Wait until Ogmios is ready :: ${e.message}`);
        await scheduler.wait(5_000);
      } else throw e;
    }
  }
}
