// What `vite.config.ts` writes into the build, for what runs the source compiled by tsc instead:
// the tests and the examples, which load this first with `node --import`.

import { readFileSync } from "node:fs";

const { version } = JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as {
  version: string;
};

Object.assign(globalThis, { __SDK_VERSION__: version });
