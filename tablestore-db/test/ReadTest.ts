import chaiAsPromised from "chai-as-promised";

import type { Suite } from "mocha";
import { expect, assert, use } from "chai";
import { db } from "./Models";
import { expectError } from "./ExceptError";

use(chaiAsPromised);

// eslint-disable-next-line func-names
describe("read test", function (this: Suite): void {
    this.timeout(10 * 60 * 1000);

    it("get results basicly", async () => {
        const snapshotDB = db.model("snapshots");
        const snapshotModel = {
            sliceUUID: "slice",
            timestamp: 1000,
            frameId: 0,
            createdAt: 1024,
        };
        await snapshotDB.set.all().deleteAll();
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-1" });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-2", timestamp: 2000 });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-3" });

        expect((
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").equals(2000)
                .and.colume("roomUUID").equals("room-2").result()
        ))
            .deep.equals({ ...snapshotModel, roomUUID: "room-2", timestamp: 2000 });

        expect((
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").greaterOrEqualsThan(1000)
                .and.colume("roomUUID").greaterOrEqualsThan("room-2").results()
        ))
            .deep.equals([
                { ...snapshotModel, roomUUID: "room-3", timestamp: 1000 },
                { ...snapshotModel, roomUUID: "room-2", timestamp: 2000 },
            ]);

        expect((
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").greaterOrEqualsThan(1000)
                .ascending().results()
        ))
            .deep.equals([
                { ...snapshotModel, roomUUID: "room-1", timestamp: 1000 },
                { ...snapshotModel, roomUUID: "room-3", timestamp: 1000 },
                { ...snapshotModel, roomUUID: "room-2", timestamp: 2000 },
            ]);

        expect((
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").greaterOrEqualsThan(1000)
                .descending().results()
        ))
            .deep.equals([
                { ...snapshotModel, roomUUID: "room-2", timestamp: 2000 },
                { ...snapshotModel, roomUUID: "room-3", timestamp: 1000 },
                { ...snapshotModel, roomUUID: "room-1", timestamp: 1000 },
            ]);

        expect(await snapshotDB.get.all().count()).equals(3);
        expect((
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").greaterOrEqualsThan(1000)
                .and.colume("roomUUID").greaterOrEqualsThan("room-2").count()
        )).equals(2);

        expect((
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").greaterOrEqualsThan(1000)
                .ascending().limit(2)
                .results()
        ))
            .deep.equals([
                { ...snapshotModel, roomUUID: "room-1", timestamp: 1000 },
                { ...snapshotModel, roomUUID: "room-3", timestamp: 1000 },
            ]);

        expect((
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").greaterOrEqualsThan(1000)
                .descending().limit(1)
                .results()
        ))
            .deep.equals([
                { ...snapshotModel, roomUUID: "room-2", timestamp: 2000 },
            ]);
    });

    it("get results splices", async () => {
        const snapshotDB = db.model("snapshots");
        const snapshotModel = {
            sliceUUID: "slice",
            timestamp: 1000,
            frameId: 0,
            createdAt: 1024,
        };
        await snapshotDB.set.all().deleteAll();
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-1" });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-2" });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-3" });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-4" });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-5" });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-6" });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-7" });
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-8" });

        // 一个 slices 刚好处理完
        let slicesCallbackCount = 0;
        expect(await snapshotDB.get.all().slices(8).resultSlices(async (models) => {
            slicesCallbackCount += 1;
            expect(slicesCallbackCount, "slices can only callback once").equals(1);
            expect(models).deep.equals([
                { ...snapshotModel, roomUUID: "room-1" },
                { ...snapshotModel, roomUUID: "room-2" },
                { ...snapshotModel, roomUUID: "room-3" },
                { ...snapshotModel, roomUUID: "room-4" },
                { ...snapshotModel, roomUUID: "room-5" },
                { ...snapshotModel, roomUUID: "room-6" },
                { ...snapshotModel, roomUUID: "room-7" },
                { ...snapshotModel, roomUUID: "room-8" },
            ]);
        })).equals(8);
        expect(slicesCallbackCount, "slices can only callback once").equals(1);

        // 两个 slices 刚好处理完，且都等长
        slicesCallbackCount = 0;
        expect(await snapshotDB.get.all().slices(4).resultSlices(async (models) => {
            slicesCallbackCount += 1;
            assert(slicesCallbackCount <= 2, "slices can only callback twice");
            if (slicesCallbackCount === 1) {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-1" },
                    { ...snapshotModel, roomUUID: "room-2" },
                    { ...snapshotModel, roomUUID: "room-3" },
                    { ...snapshotModel, roomUUID: "room-4" },
                ]);
            } else {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-5" },
                    { ...snapshotModel, roomUUID: "room-6" },
                    { ...snapshotModel, roomUUID: "room-7" },
                    { ...snapshotModel, roomUUID: "room-8" },
                ]);
            }
        })).equals(8);
        expect(slicesCallbackCount, "slices can only callback twice").equals(2);

        // 两个 slices 刚好处理完，但第二个没有装满
        slicesCallbackCount = 0;
        expect(await snapshotDB.get.all().slices(6).resultSlices(async (models) => {
            slicesCallbackCount += 1;
            assert(slicesCallbackCount <= 2, "slices can only callback twice");
            if (slicesCallbackCount === 1) {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-1" },
                    { ...snapshotModel, roomUUID: "room-2" },
                    { ...snapshotModel, roomUUID: "room-3" },
                    { ...snapshotModel, roomUUID: "room-4" },
                    { ...snapshotModel, roomUUID: "room-5" },
                    { ...snapshotModel, roomUUID: "room-6" },
                ]);
            } else {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-7" },
                    { ...snapshotModel, roomUUID: "room-8" },
                ]);
            }
        })).equals(8);
        expect(slicesCallbackCount, "slices can only callback twice").equals(2);

        // 三个 slices 刚好处理完，最后一个没装满
        slicesCallbackCount = 0;
        expect(await snapshotDB.get.all().slices(3).resultSlices(async (models) => {
            slicesCallbackCount += 1;
            assert(slicesCallbackCount <= 3, "slices can only callback third");
            if (slicesCallbackCount === 1) {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-1" },
                    { ...snapshotModel, roomUUID: "room-2" },
                    { ...snapshotModel, roomUUID: "room-3" },
                ]);
            } else if (slicesCallbackCount === 2) {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-4" },
                    { ...snapshotModel, roomUUID: "room-5" },
                    { ...snapshotModel, roomUUID: "room-6" },
                ]);
            } else {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-7" },
                    { ...snapshotModel, roomUUID: "room-8" },
                ]);
            }
        })).equals(8);
        expect(slicesCallbackCount, "slices can only callback third").equals(3);

        // 限制个数
        slicesCallbackCount = 0;
        expect(await snapshotDB.get.all().slices(4).limit(6).resultSlices(async (models) => {
            slicesCallbackCount += 1;
            assert(slicesCallbackCount <= 2, "slices can only callback twice");
            if (slicesCallbackCount === 1) {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-1" },
                    { ...snapshotModel, roomUUID: "room-2" },
                    { ...snapshotModel, roomUUID: "room-3" },
                    { ...snapshotModel, roomUUID: "room-4" },
                ]);
            } else {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-5" },
                    { ...snapshotModel, roomUUID: "room-6" },
                ]);
            }
        })).equals(6);
        expect(slicesCallbackCount, "slices can only callback twice").equals(2);

        // 通过 stop() 来停止
        slicesCallbackCount = 0;
        expect(await snapshotDB.get.all().slices(3).resultSlices(async (models, stop) => {
            slicesCallbackCount += 1;
            assert(slicesCallbackCount <= 2, "slices can only callback twice");
            if (slicesCallbackCount === 1) {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-1" },
                    { ...snapshotModel, roomUUID: "room-2" },
                    { ...snapshotModel, roomUUID: "room-3" },
                ]);
            } else {
                expect(models).deep.equals([
                    { ...snapshotModel, roomUUID: "room-4" },
                    { ...snapshotModel, roomUUID: "room-5" },
                    { ...snapshotModel, roomUUID: "room-6" },
                ]);
                stop();
            }
        })).equals(6);
        expect(slicesCallbackCount, "slices can only callback twice").equals(2);
    });

    it("invalid get", async () => {
        const snapshotDB = db.model("snapshots");
        const snapshotModel = {
            sliceUUID: "slice",
            timestamp: 1000,
            frameId: 0,
            createdAt: 1024,
        };
        await snapshotDB.set.all().deleteAll();
        await snapshotDB.post({ ...snapshotModel, roomUUID: "room-1" });

        await expectError("cannot includes all", async () => {
            await snapshotDB.get.all().result();
        });
        await expectError("expect \"=\", invalid sign of \"timestamp\": >=", async () => {
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").greaterOrEqualsThan(1000)
                .and.colume("roomUUID").equals("room-1").result();
        });
        await expectError("lost primary key \"roomUUID\"", async () => {
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").equals(1000).result();
        });
        expect((
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").equals(1000).results()
        )).deep.equals([{ ...snapshotModel, roomUUID: "room-1" }]);

        await expectError("lost primary key \"timestamp\"", async () => {
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("roomUUID").equals("room-1").result();
        });
        await expectError("invalid condition columes list: sliceUUID = slice && roomUUID = room-1", async () => {
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("roomUUID").equals("room-1").results();
        });

        await expectError("invalid value of \"timestamp\": foobar", async () => {
            await snapshotDB.get.colume("sliceUUID").equals("slice")
                .and.colume("timestamp").equals("foobar" as any)
                .and.colume("roomUUID").equals("room-1").results();
        });
    });
});
