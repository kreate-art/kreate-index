import { debounce } from "throttle-debounce";

import { Sql } from ".";

const DEFAULT_DEBOUNCE = 500; // 0.5s
const DEFAULT_CONCURRENTLY = false;

export interface ViewsRefresher {
  refresh(view: string): void;
  refreshImmediately(view: string): Promise<void>;
  shutdown(): void;
}

export type ViewsRefresherOptionsEntry = {
  debounce?: number;
  concurrently?: boolean;
};

export type ViewsRefresherOptions = {
  default?: ViewsRefresherOptionsEntry;
  views?: Record<string, ViewsRefresherOptionsEntry>;
};

export function createViewsRefresher(
  sql: Sql,
  options?: ViewsRefresherOptions
): ViewsRefresher {
  const defaultOption = options?.default;
  const defaultDebounce = defaultOption?.debounce ?? DEFAULT_DEBOUNCE;
  const defaultConcurrently =
    defaultOption?.concurrently ?? DEFAULT_CONCURRENTLY;
  const viewsOptions = options?.views ?? {};

  const refreshes: Record<string, debounce<() => Promise<void>>> = {};

  let isActive = true;

  async function doRefreshNormally(view: string) {
    if (!isActive) {
      console.warn(`<viewsRefresher> Inactive. Ignored Refresh: ${view}`);
      return;
    }
    console.log(`<viewsRefresher> Refresh: ${view}`);
    await sql`REFRESH MATERIALIZED VIEW ${sql(view)}`;
  }

  async function doRefreshConcurrently(view: string) {
    if (!isActive) {
      console.warn(
        `<viewsRefresher> Inactive. Ignored Refresh Concurrently: ${view}`
      );
      return;
    }
    console.log(`<viewsRefresher> Refresh Concurrently: ${view}`);
    await sql`REFRESH MATERIALIZED VIEW CONCURRENTLY ${sql(view)}`;
  }

  return {
    refresh: function (view) {
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
    },
    refreshImmediately: async function (view) {
      refreshes[view]?.cancel({ upcomingOnly: true });
      if (viewsOptions[view]?.concurrently ?? defaultConcurrently) {
        await doRefreshConcurrently(view);
      } else {
        await doRefreshNormally(view);
      }
    },
    shutdown: function () {
      isActive = false;
    },
  };
}
