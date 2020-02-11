import chaiAsPromised from "chai-as-promised";

import type { Suite } from "mocha";
import { expect, use } from "chai";

import type { AccessKeyModel } from "./Models";
import { db } from "./Models";
import { expectError } from "./ExceptError";

use(chaiAsPromised);

// eslint-disable-next-line func-names
describe("index test", function (this: Suite): void {
    this.timeout(10 * 60 * 1000);

    it("get elements by index", async () => {
        const accessDB = db.model("accessKeys");
        const currentDate = new Date();

        await accessDB.set.all().deleteAll();

        await accessDB.post({
            ak: "AK-1",
            sk: "SK-1",
            appUUID: "link.netless",
            teamUUID: "001",
            isBan: false,
            createdAt: currentDate,
        });
        await accessDB.post({
            ak: "AK-2",
            sk: "SK-2",
            appUUID: "link.netless",
            teamUUID: "002",
            isBan: false,
            createdAt: currentDate,
        });
        await accessDB.post({
            ak: "AK-3",
            sk: "SK-3",
            appUUID: "link.netless",
            teamUUID: "001",
            isBan: false,
            createdAt: currentDate,
        });

        expect(await accessDB.get.colume("ak").equals("AK-1").result()).deep.equals({
            ak: "AK-1",
            sk: "SK-1",
            appUUID: "link.netless",
            teamUUID: "001",
            isBan: false,
            createdAt: currentDate,
        });
        expect(await accessDB.get.colume("teamUUID").equals("001").result()).deep.equals({
            ak: "AK-1",
            sk: "SK-1",
            appUUID: "link.netless",
            teamUUID: "001",
            isBan: false,
            createdAt: currentDate,
        });
        expect(await accessDB.get.colume("teamUUID").equals("001").results()).deep.equals([
            {
                ak: "AK-1",
                sk: "SK-1",
                appUUID: "link.netless",
                teamUUID: "001",
                isBan: false,
                createdAt: currentDate,
            },
            {
                ak: "AK-3",
                sk: "SK-3",
                appUUID: "link.netless",
                teamUUID: "001",
                isBan: false,
                createdAt: currentDate,
            },
        ]);
        const results: AccessKeyModel[] = [];

        await accessDB.get.colume("teamUUID").equals("001").slices(1).resultSlices(async (slices) => {
            expect(slices.length).equals(1);
            results.push(...slices);
        });
        expect(results).deep.equals([
            {
                ak: "AK-1",
                sk: "SK-1",
                appUUID: "link.netless",
                teamUUID: "001",
                isBan: false,
                createdAt: currentDate,
            },
            {
                ak: "AK-3",
                sk: "SK-3",
                appUUID: "link.netless",
                teamUUID: "001",
                isBan: false,
                createdAt: currentDate,
            },
        ]);

        await accessDB.set.colume("teamUUID").equals("002").patch({
            appUUID: "group.netless",
            isBan: true,
        });
        expect(await accessDB.get.colume("teamUUID").equals("002").results()).deep.equals([
            {
                ak: "AK-2",
                sk: "SK-2",
                appUUID: "group.netless",
                teamUUID: "002",
                isBan: true,
                createdAt: currentDate,
            },
        ]);
        await expectError("not unique conditions: teamUUID = 001", async () => {
            await accessDB.set.colume("teamUUID").equals("001").patch({
                appUUID: "group.netless",
                isBan: true,
            });
        });
        await accessDB.set.colume("ak").equals("AK-3").patch({
            teamUUID: "002",
            isBan: true,
        });
        expect(await accessDB.get.colume("teamUUID").equals("002").results()).deep.equals([
            {
                ak: "AK-2",
                sk: "SK-2",
                appUUID: "group.netless",
                teamUUID: "002",
                isBan: true,
                createdAt: currentDate,
            },
            {
                ak: "AK-3",
                sk: "SK-3",
                appUUID: "link.netless",
                teamUUID: "002",
                isBan: true,
                createdAt: currentDate,
            },
        ]);
        await accessDB.set.colume("teamUUID").equals("001").delete();

        expect(await accessDB.get.colume("teamUUID").equals("001").results()).deep.equals([]);
        expect(await accessDB.get.colume("teamUUID").equals("002").results()).deep.equals([
            {
                ak: "AK-2",
                sk: "SK-2",
                appUUID: "group.netless",
                teamUUID: "002",
                isBan: true,
                createdAt: currentDate,
            },
            {
                ak: "AK-3",
                sk: "SK-3",
                appUUID: "link.netless",
                teamUUID: "002",
                isBan: true,
                createdAt: currentDate,
            },
        ]);
        expect(await accessDB.get.colume("teamUUID").equals("002")
                             .and.colume("ak").equals("AK-2").results()).deep.equals([
            {
                ak: "AK-2",
                sk: "SK-2",
                appUUID: "group.netless",
                teamUUID: "002",
                isBan: true,
                createdAt: currentDate,
            },
        ]);

        await expectError("not unique conditions: teamUUID = 002", async () => {
            await accessDB.set.colume("teamUUID").equals("002").delete();
        });

        await accessDB.set.colume("teamUUID").equals("002").deleteAll();
        expect(await accessDB.get.colume("teamUUID").equals("001").results()).deep.equals([]);
        expect(await accessDB.get.colume("teamUUID").equals("002").results()).deep.equals([]);
        expect(await accessDB.get.all().results()).deep.equals([]);
    });
});
