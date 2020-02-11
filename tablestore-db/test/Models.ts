import { Database } from "netless-dictionary-db";
import { TablestoreAdapterFactory, TableStoreType } from "../src/index";

export type TestModels = {
    readonly rooms: RoomModel;
    readonly snapshots: SnapshotModel;
    readonly members: MemberModel;
    readonly accessKeys: AccessKeyModel;
};

export type RoomModel = {
    readonly uuid: string;
    readonly akkoVersion: string;
    readonly state: "active" | "zombie" | "ban";
    readonly isBan: boolean;
    readonly usersMaxCount: number;
    readonly rate: number;
    readonly createdAt: Date;
};

export type SnapshotModel = {
    readonly sliceUUID: string;
    readonly timestamp: number;
    readonly roomUUID: string;
    readonly frameId: number;
    readonly createdAt: number;
};

export type MemberModel = {
    readonly id: string;
    readonly name: string;
    readonly age?: number;
    readonly city: string;
};

export type AccessKeyModel = {
    readonly ak: string;
    readonly sk: string;
    readonly appUUID: string;
    readonly teamUUID: string;
    readonly isBan: boolean;
    readonly createdAt: Date;
};

const adapterFactory = new TablestoreAdapterFactory<TestModels>({
    "rooms": {
        keys: {
            "uuid": TableStoreType.string,
        },
        columes: {
            "akkoVersion": TableStoreType.string,
            "state": TableStoreType.enums(["active", "zombie", "ban"]),
            "isBan": TableStoreType.boolean,
            "usersMaxCount": TableStoreType.integer,
            "rate": TableStoreType.float,
            "createdAt": TableStoreType.date,
        },
    },
    "snapshots": {
        keys: {
            "sliceUUID": TableStoreType.string,
            "timestamp": TableStoreType.integer,
            "roomUUID": TableStoreType.string,
        },
        columes: {
            "frameId": TableStoreType.integer,
            "createdAt": TableStoreType.integer,
        },
    },
    "members": {
        keys: {
            "id": TableStoreType.string,
        },
        columes: {
            "name": TableStoreType.string,
            "age": TableStoreType.integerOptional,
            "city": TableStoreType.stringDefaultValue("Shanghai"),
        },
    },
    "accessKeys": {
        keys: {
            "ak": TableStoreType.string,
        },
        columes: {
            "sk": TableStoreType.string,
            "appUUID": TableStoreType.string,
            "teamUUID": TableStoreType.string,
            "isBan": TableStoreType.boolean,
            "createdAt": TableStoreType.date,
        },
        indexes: {
            "accessKeys_teamUUID_index": ["teamUUID"],
        },
    },
});

export const db: Database<TestModels> = new Database(adapterFactory.create({
    accessKeyId: "***",
    secretAccessKey: "***",
    instancename: "unit-test",
    endpoint: "https://wrcev2.cn-hangzhou.ots.aliyuncs.com",
}));
