type Entries<T> = {
  [P in keyof T]-?: [P, T[P]];
}[keyof T][];

export const objectEntries = <T extends object>(obj: T) =>
  Object.entries(obj) as Entries<T>;

export const objectKeys = <T extends object>(obj: T) =>
  Object.keys(obj) as (keyof T)[];

export function noop() {
  /* ignored */
}

export async function noop$async() {
  /* ignored */
}

export function cached<T>(fn: () => T): () => T {
  let value: T | undefined = undefined;
  return () => {
    if (value === undefined) value = fn();
    return value;
  };
}

/**
 * This function is brought from kreate-web
 *
 * Shorten integers.
 *
 * Examples:
 * - 5813 --> 5.8K
 * - 9311836474 --> 931.2T
 */
export function shortenNumber(
  value: number | bigint,
  options?: { shift?: number }
) {
  // Because we are only displaying numbers, it's okay to lose
  // precision when converting bigint to number.
  const actualNumber = Number(value) * Math.pow(10, options?.shift ?? 0);
  return Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(actualNumber);
}
