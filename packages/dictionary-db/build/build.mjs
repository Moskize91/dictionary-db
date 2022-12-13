import esbuild from "esbuild";
import rimraf from "rimraf";

import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { nodeExternalsPlugin } from "esbuild-node-externals";

const packagePath = resolve(dirname(fileURLToPath(import.meta.url)), "../");
const outputPath = resolve(packagePath, "./dist");

async function build() {
    rimraf.sync(outputPath);

    await esbuildBuild("esm", "index.esm.mjs");
    await esbuildBuild("cjs", "index.cjs.js");
}

function esbuildBuild(format, outputFile) {
    return esbuild.build({
        entryPoints: [resolve(packagePath, "./src/index.ts")],
        outfile: resolve(outputPath, outputFile),
        format,
        target: ["es2017", "node16"],
        sourcemap: false,
        bundle: true,
        minify: true,
        plugins: [nodeExternalsPlugin({
            packagePath: resolve(packagePath, "./package.json"),
        })],
    });
}

build().catch((error) => {
    console.error(error);
    process.exit(1);
});
