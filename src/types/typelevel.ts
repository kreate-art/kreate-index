export type Simplify<T> = T extends object ? { [K in keyof T]: T[K] } : T;

// It basically does nothing, just used for documentation
export type NonEmpty<T> = T;

export type WithId<T, I = bigint> = T & { id: I };

export type MaybePromise<T> = T | Promise<T>;

type Entries<T> = {
  [P in keyof T]-?: [P, T[P]];
}[keyof T][];

export const objectEntries = <T extends object>(obj: T) =>
  Object.entries(obj) as Entries<T>;

export const objectKeys = <T extends object>(obj: T) =>
  Object.keys(obj) as (keyof T)[];

export function noop() {
  // Ignored
}

export async function noop$async() {
  // Ignored
}
