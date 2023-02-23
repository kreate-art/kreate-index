/**
 * This function is brought from teiki-web
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
