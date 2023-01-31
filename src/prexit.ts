import prexitDef, * as prexitMod from "prexit";

// TODO: prexit types are kinda broken
export type Prexit = typeof prexitDef & typeof prexitMod & { exit0(): void };

const prexit = prexitDef as Prexit;
prexit.exit0 = function () {
  // process.exitCode = 0;
  prexit.exit(0);
};

export default prexit;
