import esbuild from "esbuild";
import rimraf from "rimraf";

import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { nodeExternalsPlugin } from "esbuild-node-externals";

const packagePath = resolve(dirname(fileURLToPath(import.meta.url)), "../");
const outputPath = resolve(packagePath, "./test/dist");

async function build() {
    rimraf.sync(outputPath);

    await esbuild.build({
        entryPoints: [resolve(packagePath, "./test/index.ts")],
        outfile: resolve(outputPath, "index.js"),
        format: "cjs",
        target: ["es2017", "node16"],
        sourcemap: true,
        bundle: true,
        minify: true,
        plugins: [nodeExternalsPlugin({
            packagePath: resolve(packagePath, "./package.json"),
            allowList: ["dictionary-db"],
        })],
    });
}

build().catch((error) => {
    console.error(error);
    process.exit(1);
});
