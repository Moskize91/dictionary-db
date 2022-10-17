import { Database } from "netless-dictionary-db";
import type { TableStoreModelDefinition } from "../src/index";
import { DatabaseAdapterFactory, TableStoreType } from "../src/index";

export type TestModels = {
    readonly rooms: RoomModel;
    readonly snapshots: SnapshotModel;
    readonly members: MemberModel;
    readonly accessKeys: AccessKeyModel;
    readonly roomStates: RoomStatesModel;
    readonly sliceBegins: SliceBeginModel;
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

export type SliceBeginModel = {
    roomUUID: string;
    beginAt: number;
    uuid: string;
};

export type RoomStatesModel = {
    readonly timestamp: number;
    readonly uuid: string;
    readonly teamId: string;
    readonly appUUID: string;
    readonly state: "active" | "release" | "recordOn" | "recordOff";
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

const modeDefinition: TableStoreModelDefinition<TestModels> = {
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
    // 这个数据库不存在，主要是为了测试 condition 操作逻辑
    "roomStates": {
        keys: {
            timestamp: TableStoreType.integer,
            uuid: TableStoreType.string,
        },
        columes: {
            teamId: TableStoreType.string,
            appUUID: TableStoreType.stringDefaultValue("unknown"),
            state: TableStoreType.enums(["active", "release", "recordOn", "recordOff"]),
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
    "sliceBegins": {
        keys: {
            "roomUUID": TableStoreType.string,
            "beginAt": TableStoreType.integer,
            "uuid": TableStoreType.string,
        },
        columes: {},
    },
};

const db: Database<TestModels> = new Database(new DatabaseAdapterFactory<TestModels>(modeDefinition).create({
    accessKeyId: "***",
    secretAccessKey: "***",
    instancename: "unit-test",
    endpoint: "https://wrcev2.cn-hangzhou.ots.aliyuncs.com",
}));

const db2: Database<TestModels> = new Database(new DatabaseAdapterFactory<TestModels>(modeDefinition).create({
    dynamodb: {
        region: "ap-northeast-1",
        credentials: {
            accessKeyId: "***",
            secretAccessKey: "***",
        },
    },
}));

export const databaseSet = { tablestore: db, dynamodb: db2 };
