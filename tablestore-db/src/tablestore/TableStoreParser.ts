import TableStore from "tablestore";

import type { Condition, Conditions } from "netless-dictionary-db";
import type { TableStoreModel, TableStoreTypeNode } from "../TableStoreType";
import type { KeysDescription } from "./ModelNode";
import { conditionsToString } from "../ConditionPrinter";

export type ModelProperties = {
    readonly primaryKeys: any[];
    readonly attributeColumns: any[];
};

export function parseModelObject<MODEL extends { [key: string]: any }>(
    { keys, columes }: TableStoreModel<MODEL>, target: MODEL,
): ModelProperties {
    const primaryKeys: any[] = [];
    const attributeColumns: any[] = [];

    for (const primaryKey in keys) {
        const keyStruct = keys[primaryKey]!;
        const value = target[primaryKey as keyof MODEL];
        const dbValue = keyStruct.toTableStoreValue(value as any);

        if (dbValue === null) {
            throw new Error(`lost primary key ${primaryKey}`);
        }
        primaryKeys.push({ [primaryKey]: dbValue });
    }
    for (const columeName in columes) {
        const columeStruct = columes[columeName]!;
        const value = target[columeName as keyof MODEL];
        const dbValue = columeStruct.toTableStoreValue(value as any);

        if (dbValue !== null) {
            attributeColumns.push({ [columeName]: dbValue });
        }
    }
    for (const key in target) {
        if (!(key in keys || key in columes)) {
            throw new Error(`unexpect colume name ${key}`);
        }
    }
    return { primaryKeys, attributeColumns };
}

export function parseModelProperties({ keys, columes }: TableStoreModel<any>,
                                     { primaryKeys, attributeColumns }: ModelProperties): any {
    const mergedObject: any = {};

    for (const { name, value } of primaryKeys) {
        const keyStruct = keys[name];
        if (!keyStruct) {
            throw new Error(`invalid primaray key ${JSON.stringify(name)}`);
        }
        mergedObject[name] = keyStruct.fromTableStoreValue(value);
    }
    for (const { columnName, columnValue } of attributeColumns) {
        const columeStruct = columes[columnName];
        if (columeStruct) {
            mergedObject[columnName] = columeStruct.fromTableStoreValue(columnValue);
        }
    }
    for (const columnName in columes) {
        if (!(columnName in mergedObject)) {
            // attributeColumns 中没有出现的部分，应该当作 null 来处理
            // 因为用户可能写成 xxxDefaultValue(...) 的形式
            mergedObject[columnName] = columes[columnName]!.fromTableStoreValue(null);
        }
    }
    return mergedObject;
}

export function parseColumes<MODEL extends { [key: string]: any }>(
    keysDescription: KeysDescription<MODEL>,
    columes: Readonly<{ [K in keyof MODEL]?: TableStoreTypeNode<MODEL[K]> }>,
    { conditions, includesAll }: Conditions<MODEL>, target: Partial<MODEL>,
): any[] {
    if (includesAll) {
        throw new Error("cannot includes all");
    }
    const conditionsMap: { [primaryKey: string]: Condition<MODEL, keyof MODEL> } = {};

    for (const condition of conditions[0]!) {
        conditionsMap[condition.columeName as string] = condition;
    }
    const putColumns: any[] = [];
    const removeColumns: any[] = [];

    for (const key in target) {
        const condition = conditionsMap[key];
        const value = target[key];

        if (!keysDescription.patchableKeys[key] && condition && condition.value !== value) {
            throw new Error(`cannot change primary key ${JSON.stringify(key)}`);
        }
        const struct = columes[key];
        if (struct) {
            const dbValue = struct.toTableStoreValue(value as any);
            if (dbValue === null) {
                removeColumns.push(key);
            } else {
                putColumns.push({ [key]: dbValue });
            }
        }
    }
    const updateOfAttributeColumns: any[] = [];

    if (putColumns.length > 0) {
        updateOfAttributeColumns.push({ "PUT": putColumns });
    }
    if (removeColumns.length > 0) {
        updateOfAttributeColumns.push({ "DELETE": removeColumns });
    }
    return updateOfAttributeColumns;
}

export function parseConditionsToPrimaryKey<MODEL extends { [key: string]: any }>(
    keys: Readonly<{ [K in keyof MODEL]?: TableStoreTypeNode<MODEL[K]> }>,
    { conditions, includesAll }: Conditions<MODEL>,
): any[] {
    if (includesAll) {
        throw new Error("cannot includes all");
    }
    if (conditions.length > 1) {
        throw new Error("tablestore not supported or condition");
    }
    const andConditions = conditions[0] || [];
    const primaryKeys: any[] = [];

    for (const primaryKey in keys) {
        const condition = andConditions.find((c) => c.columeName === primaryKey);
        if (!condition) {
            throw new Error(`lost primary key ${JSON.stringify(primaryKey)}`);
        }
        if (condition.sign !== "=") {
            throw new Error(`expect "=", invalid sign of ${JSON.stringify(primaryKey)}: ${condition.sign}`);
        }
        const struct = keys[primaryKey as keyof MODEL]!;
        const value = struct.toTableStoreValue(condition.value);

        primaryKeys.push({ [primaryKey]: value });
    }
    if (andConditions.length !== primaryKeys.length) {
        // 所有的 primary key 都要出现，并且都是 "=" 符
        throw new Error(`invalid condition columes list: ${conditionsToString(conditions)}`);
    }
    return primaryKeys;
}

export type PrimaryKeyRange = {
    readonly startPrimaryKey: any[];
    readonly endPrimaryKey: any[];
};

export function parseConditionsToRange<MODEL extends { [key: string]: any }>(
    keys: Readonly<{ [K in keyof MODEL]?: TableStoreTypeNode<MODEL[K]> }>,
    { conditions }: Conditions<MODEL>,
): PrimaryKeyRange {
    if (conditions.length > 1) {
        throw new Error("tablestore not supported or condition");
    }
    const andConditions = conditions[0];
    const sortedConditions: { [primiaryKey: string]: Condition<MODEL, keyof MODEL>[] } = {};
    let sortedConditionsCount = 0;

    if (andConditions) {
        for (const primaryKey in keys) {
            let keyConditions: Condition<MODEL, keyof MODEL>[] | null = null;

            for (const condition of andConditions) {
                if (condition.columeName === primaryKey) {
                    if (!keyConditions) {
                        keyConditions = [];
                    }
                    keyConditions.push(condition);
                    sortedConditionsCount += 1;
                }
            }
            if (!keyConditions) {
                break;
            }
            sortedConditions[primaryKey] = keyConditions;
        }
        if (sortedConditionsCount !== andConditions.length) {
            // 上一个 for 循环应该将 andConditions 全部整理过去，如果没有，则说明如下情况之一发生：
            // 1. 无法识别的 colume name（未注册）
            // 2. 试图限制 attribute 的条件（tablestore 仅支持限制 primiary key）
            // 3. 跳过某个 primary key 取它之后的 primary key
            throw new Error(`invalid condition columes list: ${conditionsToString(conditions)}`);
        }
    }
    const inclusiveStartPrimaryKey: any[] = [];
    const exclusiveEndPrimaryKey: any[] = [];
    const range: PrimaryKeyRange = {
        startPrimaryKey: inclusiveStartPrimaryKey,
        endPrimaryKey: exclusiveEndPrimaryKey,
    };
    for (const primaryKey in keys) {
        const struct = keys[primaryKey as keyof MODEL]!;
        const keyConditions = sortedConditions[primaryKey];

        let startPrimaryKey: any;
        let endPrimaryKey: any;

        if (keyConditions) {
            for (const { sign, value } of keyConditions) {
                switch (sign) {
                    case "=": {
                        if (startPrimaryKey !== undefined) {
                            throw new Error(`conflicting condition combination ${JSON.stringify(primaryKey)}`);
                        }
                        if (endPrimaryKey !== undefined) {
                            throw new Error(`conflicting condition combination ${JSON.stringify(primaryKey)}`);
                        }
                        const cell = struct.toTableStoreValue(value);
                        startPrimaryKey = cell;
                        endPrimaryKey = cell;
                        break;
                    }
                    case ">": {
                        if (startPrimaryKey !== undefined) {
                            throw new Error(`conflicting condition combination ${JSON.stringify(primaryKey)}`);
                        }
                        startPrimaryKey = struct.toTableStoreValue(nextValue(value, +1) as any);
                        break;
                    }
                    case ">=": {
                        if (startPrimaryKey !== undefined) {
                            throw new Error(`conflicting condition combination ${JSON.stringify(primaryKey)}`);
                        }
                        startPrimaryKey = struct.toTableStoreValue(value);
                        break;
                    }
                    case "<": {
                        if (endPrimaryKey !== undefined) {
                            throw new Error(`conflicting condition combination ${JSON.stringify(primaryKey)}`);
                        }
                        endPrimaryKey = struct.toTableStoreValue(nextValue(value, -1) as any);
                        break;
                    }
                    case "<=": {
                        if (endPrimaryKey !== undefined) {
                            throw new Error(`conflicting condition combination ${JSON.stringify(primaryKey)}`);
                        }
                        endPrimaryKey = struct.toTableStoreValue(value);
                        break;
                    }
                    default: {
                        throw new Error(`invalid sign of ${JSON.stringify(primaryKey)}: ${sign}`);
                    }
                }
            }
        }
        if (startPrimaryKey === undefined) {
            startPrimaryKey = TableStore.INF_MIN;
        }
        if (endPrimaryKey === undefined) {
            endPrimaryKey = TableStore.INF_MAX;
        }
        inclusiveStartPrimaryKey.push({ [primaryKey]: startPrimaryKey });
        exclusiveEndPrimaryKey.push({ [primaryKey]: endPrimaryKey });
    }
    return range;
}

function nextValue(value: any, increment: number): number {
    if (!Number.isSafeInteger(value)) {
        throw new Error("value can be only integer:" + value);
    }
    return value + increment;
}
