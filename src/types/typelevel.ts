export type Simplify<T> = T extends object ? { [K in keyof T]: T[K] } : T;

// It basically does nothing, just used for documentation
export type NonEmpty<T> = T;

export type MaybePromise<T> = T | Promise<T>;

type Entries<T> = {
  [K in keyof T]: [K, T[K]];
}[keyof T][];

export const objectEntries = <T extends object>(obj: T) =>
  Object.entries(obj) as Entries<T>;

export function noop() {
  // Ignored
}

export async function noop$async() {
  // Ignored
}
