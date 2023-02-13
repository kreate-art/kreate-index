import { debounce } from "throttle-debounce";

import { Sql } from ".";

const DEFAULT_DEBOUNCE = 500; // 0.5s
const DEFAULT_CONCURRENTLY = false;

export interface ViewsController {
  refresh(view: string): void;
  refreshImmediately(view: string): Promise<void>;
  shutdown(): void;
}

export type ViewsControllerOptionsEntry = {
  debounce?: number;
  concurrently?: boolean;
  dependencies?: string[];
};

export type ViewsControllerOptions = {
  default?: ViewsControllerOptionsEntry;
  views?: Record<string, ViewsControllerOptionsEntry>;
};

export function createViewsController(
  sql: Sql,
  options?: ViewsControllerOptions
): ViewsController {
  const defaultOption = options?.default;
  const defaultDebounce = defaultOption?.debounce ?? DEFAULT_DEBOUNCE;
  const defaultConcurrently =
    defaultOption?.concurrently ?? DEFAULT_CONCURRENTLY;
  const viewsOptions = options?.views ?? {};

  const refreshes: Record<string, debounce<() => Promise<void>>> = {};

  const depends = new Map<string, string[]>();
  for (const [view, { dependencies }] of Object.entries(viewsOptions)) {
    if (!dependencies) continue;
    for (const parent of dependencies) {
      let children = depends.get(parent);
      if (!children) {
        children = [];
        depends.set(parent, children);
      }
      children.push(view);
    }
  }

  let isActive = true;

  async function doRefreshNormally(view: string) {
    if (!isActive) {
      console.warn(`<views> Inactive. Ignored Refresh: ${view}`);
      return;
    }
    await sql`REFRESH MATERIALIZED VIEW ${sql(view)}`;
    console.log(`<views> Refreshed: ${view}`);
    const children = depends.get(view);
    if (children) for (const child of children) refresh(child);
  }

  async function doRefreshConcurrently(view: string) {
    if (!isActive) {
      console.warn(`<views> Inactive. Ignored Refresh Concurrently: ${view}`);
      return;
    }
    await sql`REFRESH MATERIALIZED VIEW CONCURRENTLY ${sql(view)}`;
    console.log(`<views> Refreshed Concurrently: ${view}`);
    const children = depends.get(view);
    if (children) for (const child of children) refresh(child);
  }

  function refresh(view: string) {
    let refresh = refreshes[view];
    if (!refresh) {
      const opts = viewsOptions[view];
      refresh = refreshes[view] = debounce(
        opts?.debounce ?? defaultDebounce,
        opts?.concurrently ?? defaultConcurrently
          ? () => doRefreshConcurrently(view)
          : () => doRefreshNormally(view)
      );
    }
    refresh();
  }

  async function refreshImmediately(view: string) {
    refreshes[view]?.cancel({ upcomingOnly: true });
    if (viewsOptions[view]?.concurrently ?? defaultConcurrently) {
      await doRefreshConcurrently(view);
    } else {
      await doRefreshNormally(view);
    }
  }

  function shutdown() {
    isActive = false;
    for (const deb of Object.values(refreshes)) deb.cancel();
  }

  return {
    refresh,
    refreshImmediately,
    shutdown,
  };
}
