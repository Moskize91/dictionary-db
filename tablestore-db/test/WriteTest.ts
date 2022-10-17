import chaiAsPromised from "chai-as-promised";

import type { Suite } from "mocha";
import { expect, assert, use } from "chai";

import type { RoomModel } from "./Models";
import { databaseSet } from "./Models";

import { expectError } from "./ExceptError";

use(chaiAsPromised);

// eslint-disable-next-line func-names
describe("write test", function (this: Suite): void {
    this.timeout(10 * 60 * 1000);

    describe("different db", (): void => {
        for (const [name, db] of Object.entries(databaseSet)) {
            it(`create, put, patch, delete, deleteAll in ${name}`, async () => {
                const roomDB = db.model("rooms");
                await roomDB.set.all().deleteAll();

                const roomTemplate: RoomModel = {
                    uuid: "uuid-1",
                    akkoVersion: "1.4.3",
                    state: "ban",
                    isBan: false,
                    usersMaxCount: 1024,
                    rate: 0.54,
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                };

                // 测试 post 多条记录
                await roomDB.post({
                    ...roomTemplate,
                    uuid: "uuid-1",
                });
                await roomDB.post({
                    ...roomTemplate,
                    uuid: "uuid-2",
                });
                await roomDB.post({
                    ...roomTemplate,
                    uuid: "uuid-3",
                    usersMaxCount: 0,
                });
                expect(await roomDB.get.all().ascending().results()).deep.equals([{
                    uuid: "uuid-1",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.54,
                    state: "ban",
                    usersMaxCount: 1024,
                }, {
                    uuid: "uuid-2",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.54,
                    state: "ban",
                    usersMaxCount: 1024,
                }, {
                    uuid: "uuid-3",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.54,
                    state: "ban",
                    usersMaxCount: 0,
                }]);

                // 测试 put 单条数据
                await roomDB.set.colume("uuid").equals("uuid-2").put({
                    uuid: "uuid-2",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-10-20T00:30:00.000Z"),
                    isBan: true,
                    rate: 1.0,
                    state: "active",
                    usersMaxCount: 0,
                });
                expect(await roomDB.get.all().ascending().results()).deep.equals([{
                    uuid: "uuid-1",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.54,
                    state: "ban",
                    usersMaxCount: 1024,
                }, {
                    uuid: "uuid-2",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-10-20T00:30:00.000Z"),
                    isBan: true,
                    rate: 1.0,
                    state: "active",
                    usersMaxCount: 0,
                }, {
                    uuid: "uuid-3",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.54,
                    state: "ban",
                    usersMaxCount: 0,
                }]);

                // 测试 patch 多条数据
                await roomDB.set.colume("uuid").equals("uuid-1").patch({
                    uuid: "uuid-1",
                    akkoVersion: "1.4.1",
                    rate: 0.8,
                });
                await roomDB.set.colume("uuid").equals("uuid-2").patch({
                    akkoVersion: "1.4.1",
                });
                await roomDB.set.colume("uuid").equals("uuid-3").patch({
                    isBan: true,
                });
                expect(await roomDB.get.all().ascending().results()).deep.equals([{
                    uuid: "uuid-1",
                    akkoVersion: "1.4.1",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.8,
                    state: "ban",
                    usersMaxCount: 1024,
                }, {
                    uuid: "uuid-2",
                    akkoVersion: "1.4.1",
                    createdAt: new Date("2020-10-20T00:30:00.000Z"),
                    isBan: true,
                    rate: 1.0,
                    state: "active",
                    usersMaxCount: 0,
                }, {
                    uuid: "uuid-3",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: true,
                    rate: 0.54,
                    state: "ban",
                    usersMaxCount: 0,
                }]);

                // 插入更多备用
                await roomDB.post({
                    ...roomTemplate,
                    uuid: "uuid-5",
                });
                await roomDB.post({
                    ...roomTemplate,
                    uuid: "uuid-8",
                    usersMaxCount: 0,
                });
                expect(await roomDB.get.all().ascending().results()).deep.equals([{
                    uuid: "uuid-1",
                    akkoVersion: "1.4.1",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.8,
                    state: "ban",
                    usersMaxCount: 1024,
                }, {
                    uuid: "uuid-2",
                    akkoVersion: "1.4.1",
                    createdAt: new Date("2020-10-20T00:30:00.000Z"),
                    isBan: true,
                    rate: 1.0,
                    state: "active",
                    usersMaxCount: 0,
                }, {
                    uuid: "uuid-3",
                    akkoVersion: "1.4.3",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: true,
                    rate: 0.54,
                    state: "ban",
                    usersMaxCount: 0,
                }, {
                    uuid: "uuid-5",
                    akkoVersion: "1.4.3",
                    state: "ban",
                    isBan: false,
                    usersMaxCount: 1024,
                    rate: 0.54,
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                }, {
                    uuid: "uuid-8",
                    akkoVersion: "1.4.3",
                    state: "ban",
                    isBan: false,
                    usersMaxCount: 0,
                    rate: 0.54,
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                }]);

                // 删除单个
                assert(await roomDB.set.colume("uuid").equals("uuid-3").delete());
                expect(await roomDB.get.all().ascending().results()).deep.equals([{
                    uuid: "uuid-1",
                    akkoVersion: "1.4.1",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.8,
                    state: "ban",
                    usersMaxCount: 1024,
                }, {
                    uuid: "uuid-2",
                    akkoVersion: "1.4.1",
                    createdAt: new Date("2020-10-20T00:30:00.000Z"),
                    isBan: true,
                    rate: 1.0,
                    state: "active",
                    usersMaxCount: 0,
                }, {
                    uuid: "uuid-5",
                    akkoVersion: "1.4.3",
                    state: "ban",
                    isBan: false,
                    usersMaxCount: 1024,
                    rate: 0.54,
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                }, {
                    uuid: "uuid-8",
                    akkoVersion: "1.4.3",
                    state: "ban",
                    isBan: false,
                    usersMaxCount: 0,
                    rate: 0.54,
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                }]);

                assert(await roomDB.set.colume("uuid").greaterOrEqualsThan("uuid-2")
                                   .and.colume("uuid").lessOrEqualsThan("uuid-6")
                                   .deleteAll());
                await roomDB.set.colume("uuid").equals("uuid-2").delete();
                await roomDB.set.colume("uuid").equals("uuid-5").delete();

                expect(await roomDB.get.all().ascending().results()).deep.equals([{
                    uuid: "uuid-1",
                    akkoVersion: "1.4.1",
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                    isBan: false,
                    rate: 0.8,
                    state: "ban",
                    usersMaxCount: 1024,
                }, {
                    uuid: "uuid-8",
                    akkoVersion: "1.4.3",
                    state: "ban",
                    isBan: false,
                    usersMaxCount: 0,
                    rate: 0.54,
                    createdAt: new Date("2020-02-01T00:00:00.000Z"),
                }]);
            });

            it(`write option in ${name}`, async () => {
                const memberDB = db.model("members");
                await memberDB.set.all().deleteAll();

                const memberMode = {
                    id: "id-1",
                    age: undefined,
                    name: "name-1",
                    city: "city1",
                };
                await memberDB.post(memberMode);

                await memberDB.set.colume("id").equals("id-1").patch({
                    age: 18,
                });
                await memberDB.set.colume("id").equals("id-1").patch({
                    age: undefined,
                    city: "a",
                });
                await memberDB.set.all().deleteAll();
            });

            it(`exception of create in ${name}`, async () => {
                const snapshotDB = db.model("snapshots");
                await snapshotDB.set.all().deleteAll();

                await expectError("invalid value of \"roomUUID\": undefined", async () => {
                    await snapshotDB.post({
                        sliceUUID: "slice-001",
                        timestamp: 110,
                        frameId: 0,
                        createdAt: 1024,
                    } as any);
                });
                await expectError("unexpect colume name otherColume", async () => {
                    await snapshotDB.post({
                        sliceUUID: "slice-001",
                        timestamp: 110,
                        roomUUID: "room-001",
                        otherColume: "hahaha",
                        frameId: 0,
                        createdAt: 1024,
                    } as any);
                });
                await expectError("invalid value of \"sliceUUID\": 770", async () => {
                    await snapshotDB.post({
                        sliceUUID: 770,
                        timestamp: 110,
                        roomUUID: "room-001",
                        frameId: 0,
                        createdAt: 1024,
                    } as any);
                });
                await expectError("invalid value of \"frameId\": frameId-0", async () => {
                    await snapshotDB.post({
                        sliceUUID: "slice-001",
                        timestamp: 110,
                        roomUUID: "room-001",
                        frameId: "frameId-0",
                        createdAt: 1024,
                    } as any);
                });

                // 第二次不能插入 primary key 完全相同的记录
                assert(await snapshotDB.post({
                    sliceUUID: "slice-001",
                    timestamp: 110,
                    roomUUID: "room-001",
                    frameId: 0,
                    createdAt: 1024,
                }));
                assert(!(await snapshotDB.post({
                    sliceUUID: "slice-001",
                    timestamp: 110,
                    roomUUID: "room-001",
                    frameId: 12,
                    createdAt: 2048,
                })));
            });

            it(`exception of put in ${name}`, async () => {
                const snapshotDB = db.model("snapshots");
                const snapshotModel = {
                    sliceUUID: "slice-001",
                    timestamp: 110,
                    roomUUID: "room-001",
                    frameId: 0,
                    createdAt: 1024,
                };
                await snapshotDB.set.all().deleteAll();
                await snapshotDB.post(snapshotModel);

                // put 的内容残缺
                await expectError("invalid value of \"createdAt\": undefined", async () => {
                    await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                    .and.colume("timestamp").equals(110)
                                    .and.colume("roomUUID").equals("room-001")
                                    .put({
                                        sliceUUID: "slice-001",
                                        timestamp: 110,
                                        roomUUID: "room-001",
                                        frameId: 200,
                                    } as any);
                });
                expect(await snapshotDB.get.colume("sliceUUID").equals("slice-001")
                                           .and.colume("timestamp").equals(110)
                                           .and.colume("roomUUID").equals("room-001").result())
                                           .deep.equals(snapshotModel);

                // put 试图修改 primary key
                await expectError("cannot change primary key \"timestamp\"", async () => {
                    await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                        .and.colume("timestamp").equals(110)
                                        .and.colume("roomUUID").equals("room-001")
                                        .put({
                                            sliceUUID: "slice-001",
                                            timestamp: 911,
                                            roomUUID: "room-002",
                                            frameId: 200,
                                            createdAt: 2048,
                                        });
                });
                expect(await snapshotDB.get.colume("sliceUUID").equals("slice-001")
                    .and.colume("timestamp").equals(110)
                    .and.colume("roomUUID").equals("room-001").result())
                    .deep.equals(snapshotModel);

                // put 不存在的内容
                assert(!(await snapshotDB.set.colume("sliceUUID").equals("slice-002")
                                         .and.colume("timestamp").equals(110)
                                         .and.colume("roomUUID").equals("room-001")
                                         .put({
                                             ...snapshotModel,
                                             sliceUUID: "slice-002",
                                         })));
            });

            it(`exception of patch in ${name}`, async () => {
                const snapshotDB = db.model("snapshots");
                const snapshotModel = {
                    sliceUUID: "slice-001",
                    timestamp: 110,
                    roomUUID: "room-001",
                    frameId: 0,
                    createdAt: 1024,
                };
                await snapshotDB.set.all().deleteAll();
                await snapshotDB.post(snapshotModel);

                // 遗漏某个 primary key
                await expectError("lost primary key \"roomUUID\"", async () => {
                    await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                    .and.colume("timestamp").equals(110)
                                    .patch({ frameId: 200 });
                });
                await expectError("lost primary key \"timestamp\"", async () => {
                    await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                    .and.colume("roomUUID").equals("room-001")
                                    .patch({ frameId: 200 });
                });
                expect(await snapshotDB.get.colume("sliceUUID").equals("slice-001")
                                       .and.colume("timestamp").equals(110)
                                       .and.colume("roomUUID").equals("room-001").result())
                                       .deep.equals(snapshotModel);

                // 试图修改 primary key
                await expectError("cannot change primary key \"roomUUID\"", async () => {
                    await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                    .and.colume("timestamp").equals(110)
                                    .and.colume("roomUUID").equals("room-001")
                                    .patch({ roomUUID: "room-002", frameId: 200 });
                });
                expect(await snapshotDB.get.colume("sliceUUID").equals("slice-001")
                                      .and.colume("timestamp").equals(110)
                                      .and.colume("roomUUID").equals("room-001").result())
                                      .deep.equals(snapshotModel);

                // 运算符出现 >=, <=, >, <, !=
                await expectError("expect \"=\", invalid sign of \"timestamp\": <", async () => {
                    await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                    .and.colume("timestamp").lessThan(110)
                                    .and.colume("roomUUID").equals("room-001")
                                    .patch({ frameId: 200 });
                });
                await expectError("expect \"=\", invalid sign of \"timestamp\": >=", async () => {
                    await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                    .and.colume("timestamp").greaterOrEqualsThan(110)
                                    .and.colume("roomUUID").equals("room-001")
                                    .patch({ frameId: 200 });
                });
                await expectError("expect \"=\", invalid sign of \"timestamp\": !=", async () => {
                    await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                    .and.colume("timestamp").notEqualTo(110)
                                    .and.colume("roomUUID").equals("room-001")
                                    .patch({ frameId: 200 });
                });
                expect(await snapshotDB.get.colume("sliceUUID").equals("slice-001")
                                       .and.colume("timestamp").equals(110)
                                       .and.colume("roomUUID").equals("room-001").result())
                                       .deep.equals(snapshotModel);

                // patch 不存在的记录
                assert(!(await snapshotDB.set.colume("sliceUUID").equals("slice-001")
                                         .and.colume("timestamp").equals(911)
                                         .and.colume("roomUUID").equals("room-001")
                                         .patch({ frameId: 200 })));
                expect(await snapshotDB.get.colume("sliceUUID").equals("slice-001")
                                       .and.colume("timestamp").equals(110)
                                       .and.colume("roomUUID").equals("room-001").result())
                                       .deep.equals(snapshotModel);
            });
        }
    });
});
