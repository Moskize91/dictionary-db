const esbuild = require("esbuild");
const path = require("path");

esbuild.build({
    bundle: true,
    entryPoints: ["test/index.ts"],
    platform: "node",
    target: "es6",
    outfile: "dist/dev/test/index.js",
    external: ["chai", "chai-as-promised", "source-map-support", "tablestore"],
    plugins: [{
        name: "filter-akko-genreated",
        setup: (build) => {
            build.onResolve({
                filter: /^netless-dictionary-db$/im
            }, () => {
                return {
                    path: path.resolve("../dictionary-db/src/index.ts"),
                    namespace: "file",
                };
            });
        },
    }],
});
