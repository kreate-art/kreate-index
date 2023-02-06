import { debounce } from "throttle-debounce";

import { assert } from "@teiki/protocol/utils";

import { Sql } from ".";

const DEFAULT_DEBOUNCE = 500; // 0.5s

export interface Notifications {
  notify(channel: string): void;
  notifyImmediately(channel: string): Promise<void>;
  listen(
    channel: string,
    onNotify: (payload: string) => void,
    onListen?: () => void
  ): Promise<() => Promise<void>>;
  shutdown(): Promise<void>;
}

export type NotificationsOptionsEntry = {
  debounce?: number;
};

export type NotificationsOptions = {
  default?: NotificationsOptionsEntry;
  channels?: Record<string, NotificationsOptionsEntry>;
};

export function createNotificationsService(
  sql: Sql,
  options?: NotificationsOptions
): Notifications {
  const defaultDebounce = options?.default?.debounce ?? DEFAULT_DEBOUNCE;
  const channelsOptions = options?.channels ?? {};

  const notifies: Record<string, debounce<() => Promise<void>>> = {};
  const unlistens = new Set<() => Promise<void>>();

  let isActive = true;
  let listenerCounter = 0;

  async function doNotify(channel: string) {
    if (!isActive) {
      console.warn(`<notifications> Inactive. Ignored Notify: ${channel}`);
      return;
    }
    console.log(`<notifications> Notify: ${channel}`);
    sql.notify(channel, "");
  }

  return {
    notify: function (channel) {
      let notify = notifies[channel];
      if (!notify)
        notify = notifies[channel] = debounce(
          channelsOptions[channel]?.debounce ?? defaultDebounce,
          () => doNotify(channel)
        );
      notify();
    },
    notifyImmediately: async function (channel) {
      notifies[channel]?.cancel({ upcomingOnly: true });
      await doNotify(channel);
    },
    listen: async function (channel, onNotify, onListen) {
      assert(isActive, `<notifications> Inactive. Ignored Listen: ${channel}`);
      const index = listenerCounter++;
      console.log(`<notifications> Listen: (${index}) ${channel}`);
      const listener = await sql.listen(channel, onNotify, onListen);
      const unlisten = async () => {
        console.log(`<notifications> Unlisten: (${index}) ${channel}`);
        await listener.unlisten();
      };
      unlistens.add(unlisten);
      return async () => {
        unlistens.delete(unlisten);
        await unlisten();
      };
    },
    shutdown: async function () {
      isActive = false;
      for (const deb of Object.values(notifies)) deb.cancel();
      await Promise.all(Array.from(unlistens).map((un) => un()));
      unlistens.clear();
    },
  };
}
