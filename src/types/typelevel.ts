export type Simplify<T> = T extends object ? { [K in keyof T]: T[K] } : T;

// It basically does nothing, just used for documentation
export type NonEmpty<T> = T;

export type WithId<T, I = bigint> = T & { id: I };

export type MaybePromise<T> = T | Promise<T>;
