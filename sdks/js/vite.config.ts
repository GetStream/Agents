import { readFileSync } from "node:fs";
import { defineConfig } from "vite";

// semantic-release writes the version it releases into package.json just before `npm publish`
// runs prepublishOnly, which runs this build: the version read here is the one published.
const manifest = JSON.parse(readFileSync(new URL("./package.json", import.meta.url), "utf8")) as {
  version: string;
  exports: Record<string, { default: string }>;
  peerDependencies: Record<string, string>;
};

// One entry per subpath in `exports`: "./dist/chat.js" is built from "src/chat.ts".
const entries = Object.fromEntries(
  Object.values(manifest.exports).map(({ default: file }) => {
    const name = file.replace(/^\.\/dist\//, "").replace(/\.js$/, "");
    return [name, `src/${name}.ts`];
  }),
);

// The peers stay the app's own copies, and Node's modules stay Node's.
const peers = Object.keys(manifest.peerDependencies);

export default defineConfig({
  define: { __SDK_VERSION__: JSON.stringify(manifest.version) },
  build: {
    outDir: "dist",
    target: "es2023",
    minify: false,
    lib: { entry: entries, formats: ["es"], fileName: (_format, name) => `${name}.js` },
    rolldownOptions: {
      external: (id) => id.startsWith("node:") || peers.some((peer) => id === peer || id.startsWith(`${peer}/`)),
      // A module per source file, laid out as tsc lays them out, so the subpaths share one copy
      // of each and `sideEffects` still names chat.js and video.js.
      output: { preserveModules: true, preserveModulesRoot: "src" },
    },
  },
});
