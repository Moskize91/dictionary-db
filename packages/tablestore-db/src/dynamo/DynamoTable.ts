/* eslint-disable prefer-destructuring */
import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import type { Condition, Conditions } from "dictionary-db";
import { conditionsToString } from "../ConditionPrinter";
import { tableStoreValueToDynamoAttr } from "./DynamoTransfer";
import type { TableStoreModel, TableStoreTypeNode } from "../TableStoreType";

export type DynamoKeyInfo<MODEL extends { [key: string]: any }> = {
    tableName: string;
    hashKey: string;
    // 永远不在 combineHashKey 中
    rangeKey?: keyof MODEL;
    // 在 key info 中 hashKey 是 combineHashKey 数组合并的关系
    combineHashKey: (keyof MODEL)[];
    priority: number;
    valueNode: TableStoreKeyMap<MODEL>;
};

// DynamoGsiInfo 中的 combineHashKey 表示这个 index 支持的 key。而 hash key 是单主键或者两个主键的合并，前者包含，并且可能多与后者。不属于 hash key 和 range
export type DynamoGsiInfo<MODEL extends { [key: string]: any }> = DynamoKeyInfo<MODEL> & {
    indexName: string;
    // 是否是原始表结构中自带的二级索引。
    // tablestore 会自动把没有列为二级索引的 primary key 作为二级索引的主键，只是排在后面。
    // 在 dynamo 中，需要把 primary key 也加上，也只取前两个主键做 hash 和 range
    isIndex: boolean;
    // key 查询制作的额外 gsi
    isGsi: boolean;
};

type DynamoTableInfo<MODEL extends { [key: string]: any }> = DynamoKeyInfo<MODEL> & {
    // 除了 tablestore 中主动设置的 index，还要针对范围查询，创建一些 dynamo 特有的 index
    indexes?: { [indexName: string]: DynamoGsiInfo<MODEL> };
};

type TableStoreKeyMap<MODEL extends { [key: string]: any }> = { [K in keyof MODEL]?: TableStoreTypeNode<MODEL[K]> };
export const splitKey = "/";
// dynamo 的 key 不能包含 '/'，支持'-'
const dynamoIndexKeySplit = "-";

export function isGsiIndex<MODEL extends { [key: string]: any }>(
    keyInfo: DynamoKeyInfo<MODEL> | DynamoGsiInfo<MODEL>,
): keyInfo is DynamoGsiInfo<MODEL> {
    if ((keyInfo as DynamoGsiInfo<MODEL>).isGsi) {
        return true;
    } else {
        return false;
    }
}

export function isTableIndex<MODEL extends { [key: string]: any }>(
    keyInfo: DynamoKeyInfo<MODEL> | DynamoGsiInfo<MODEL>,
): keyInfo is DynamoKeyInfo<MODEL> {
    if ((keyInfo as DynamoGsiInfo<MODEL>).isIndex) {
        return true;
    } else {
        return false;
    }
}

export class DynamoTable<MODEL extends { [key: string]: any }> {

    private readonly tableInfo: Readonly<DynamoTableInfo<MODEL>>;
    private readonly _keys: TableStoreKeyMap<MODEL>;
    private readonly _columns: TableStoreKeyMap<MODEL>;
    private readonly _patchableKeys: { readonly [K in keyof MODEL]: boolean };
    private readonly _indexKeyMaps?: {
        readonly [indexName: string]: ReadonlyArray<keyof MODEL>;
    };

    public constructor(public readonly name: string, model: TableStoreModel<MODEL>) {
        this._keys = model.keys;
        this._columns = model.columes;
        this._indexKeyMaps = model.indexes;
        this.tableInfo = Object.freeze(this.generateDynamoTableInfo(name, model));
        this._patchableKeys = Object.freeze(this.createPatchableKeys());
    }

    private generateDynamoTableInfo(name: string, inputModel: TableStoreModel<MODEL>): DynamoTableInfo<MODEL> {

        const primaryKeys = Object.keys(inputModel.keys) as Array<keyof MODEL>;
        const combinePkKeys: Array<keyof MODEL> = [];
        let rangeKey: keyof MODEL | undefined;
        if (primaryKeys.length === 1) {
            combinePkKeys.push(primaryKeys[0]);
        } else if (primaryKeys.length === 2) {
            rangeKey = primaryKeys[1];
            combinePkKeys.push(primaryKeys[0]);
        } else {
            // 最后一个 key 当做 range key，forin 遍历，在 js 中是根据创建顺序的
            for (const key in inputModel.keys) {
                combinePkKeys.push(key);
            }
            rangeKey = combinePkKeys.pop();
        }

        const keyInfo: DynamoKeyInfo<MODEL> = {
            tableName: name,
            hashKey: combinePkKeys.join(splitKey),
            rangeKey,
            combineHashKey: combinePkKeys,
            priority: 0,
            valueNode: inputModel.keys,
        };

        const indexes: { [indexName: string]: DynamoGsiInfo<MODEL> } = {};
        if (inputModel.indexes) {
            for (const [indexName, indexKeys] of Object.entries(inputModel.indexes)) {
                const indexKeyMap: TableStoreKeyMap<MODEL> = {} as unknown as TableStoreKeyMap<MODEL>;
                for (const key of indexKeys) {
                    indexKeyMap[key] = inputModel.keys[key] || inputModel.columes[key];
                }
                // tablestore 的 index 会自动把 primaryKey 加设置的 index key 后面
                for (const key in inputModel.keys) {
                    if (!indexKeys.includes(key)) {
                        indexKeyMap[key] = inputModel.keys[key];
                    }
                }

                indexes[indexName] = { ...this.normalGsiKeyInfo(name, indexKeyMap), indexName, isIndex: true };
            }
        }

        if (Object.keys(primaryKeys).length >= 3) {
            const primaryGsiKeyInfo = this.normalGsiKeyInfo(name, inputModel.keys);
            indexes[primaryGsiKeyInfo.indexName] = primaryGsiKeyInfo;
        }
        if (rangeKey && Object.keys(primaryKeys).length >= 4) {
            const specialGsiKeyInfo = this.combineGsiKeyInfo(name, combinePkKeys, rangeKey);
            indexes[specialGsiKeyInfo.indexName] = specialGsiKeyInfo;
        }

        return {
            ...keyInfo,
            indexes,
        };
    }

    // 当表结构中 keys 超过4个时。为了优化查询性能，额外会创建一个 前两个主键合为一个 key 的 pk，第三个做 sk 的 gsi。在提交数据时，也要创建合并 key 的数据。由于是主键，所以不会更改。
    // eslint-disable-next-line max-len
    private combineGsiKeyInfo(name: string, combineHashKey: (keyof MODEL)[], rangeKey: keyof MODEL): DynamoGsiInfo<MODEL> {
        const valueNode: TableStoreKeyMap<MODEL> = {};
        [...combineHashKey, rangeKey].forEach((key) => {
            valueNode[key] = this._keys[key] || this._columns[key];
        });
        return {
            tableName: name,
            indexName: [combineHashKey.slice(0, 2), rangeKey].join(splitKey),
            isGsi: true,
            isIndex: false,
            hashKey: combineHashKey.slice(0, 2).join(splitKey),
            rangeKey,
            combineHashKey,
            priority: 1,
            valueNode,
        };
    }

    // 当表结构中 keys 超过两个时，生成一个，第一主键为 pk，第二主键做 sk 的 gsi
    private normalGsiKeyInfo(name: string, keyMap: TableStoreKeyMap<MODEL>): DynamoGsiInfo<MODEL> {
        const keys = Object.keys(keyMap) as Array<keyof MODEL>;
        const combineHashKey = [...keys.slice(0, 1), ...keys.slice(2, keys.length)];
        const valueNode: TableStoreKeyMap<MODEL> = {};
        [...keys].forEach((key) => {
            valueNode[key] = this._keys[key] || this._columns[key];
        });
        return {
            tableName: name,
            indexName: keys.slice(0, 2).join(dynamoIndexKeySplit),
            isGsi: true,
            isIndex: false,
            hashKey: keys[0] as string,
            rangeKey: keys[1] as string,
            combineHashKey,
            priority: 2,
            valueNode,
        };
    }

    private createPatchableKeys(): { readonly [K in keyof MODEL]: boolean } {
        const keys: { [K in keyof MODEL]: boolean } = {} as any;
        for (const key in this._columns) {
            keys[key] = true;
        }
        for (const key in this._keys) {
            keys[key] = false;
        }
        return Object.freeze(keys);
    }

    // 尽量和其他公开接口，保持一致
    public get columes(): { readonly [K in keyof MODEL]?: TableStoreTypeNode<MODEL[K]> } {
        return this._columns;
    }

    public get keys(): { readonly [K in keyof MODEL]?: TableStoreTypeNode<MODEL[K]> } {
        return this._keys;
    }

    // 一个适配层，尽量保持和 tablestore 版本错误信息一致，不过仍然有一些是无法一致的。
    private preCheck(keyInfo: DynamoKeyInfo<MODEL>,
                     conditions: ReadonlyArray<ReadonlyArray<Condition<MODEL, keyof MODEL>>>): void {
        let missingKey;
        const primaryKeys = [...keyInfo.combineHashKey, keyInfo.rangeKey].filter((key): key is keyof MODEL => key !== undefined);
        for (const value of primaryKeys) {
            const hasKey = conditions.some((andConditions) => {
                return andConditions.some((condition) => {
                    return condition.columeName === value;
                });
            });
            if (hasKey && missingKey) {
                throw new Error(`lost primary key ${JSON.stringify(missingKey)}`);
            }
            if (!hasKey) {
                missingKey = value;
            }
        }
    }

    public keysDescription({ conditions, includesAll }: Conditions<MODEL>): DynamoKeyInfo<MODEL> | DynamoGsiInfo<MODEL> {
        if (includesAll) {
            return { ...this.tableInfo };
        }
        let suitableKeysDescription: DynamoKeyInfo<MODEL> | null = null;
        let currentKeysCount = Number.MAX_SAFE_INTEGER;

        const keyInfos = [this.tableInfo, ...Object.values(this.tableInfo.indexes || {}), this.scanKeyInfo];
        for (const keysDescription of keyInfos) {
            if (keysDescription.priority < currentKeysCount &&
                this.isMatch(keysDescription, conditions)) {
                suitableKeysDescription = keysDescription;
                currentKeysCount = keysDescription.priority;
            }
            // 如果命中了非 gsi 的 keyInfo，说明所有的 pk 都给了固定值，直接返回就行
            if (suitableKeysDescription && !isGsiIndex(suitableKeysDescription)) {
                return suitableKeysDescription as DynamoKeyInfo<MODEL>;
            }
        }
        if (!suitableKeysDescription) {
            throw new Error(`conditions cannot match any primary keys or index keys: ${conditionsToString(conditions)}`);
        }
        return suitableKeysDescription as DynamoGsiInfo<MODEL>;
    }

    public isIndexKey(key: string): boolean {
        for (const indexInfo of Object.values(this.tableInfo.indexes || {})) {
            const keys = [...indexInfo.combineHashKey, indexInfo.rangeKey].filter((key): key is keyof MODEL => key !== undefined);
            if (keys.includes(key)) {
                return true;
            }
        }
        return false;
    }

    private isMatch(keyInfo: DynamoKeyInfo<MODEL>,
                    orConditions: ReadonlyArray<ReadonlyArray<Condition<MODEL, keyof MODEL>>>): boolean {
        const keys = [...keyInfo.combineHashKey, keyInfo.rangeKey].filter((key): key is keyof MODEL => key !== undefined);
        for (const andConditions of orConditions) {
            for (const condition of andConditions) {
                if (!keys.includes(condition.columeName)) {
                    return false;
                }
                // hashKey 不支持 非等于表达式，不等式，需要由 rangeKey 或者 filter 过滤
                if (condition.sign !== "=" && keyInfo.hashKey.includes(condition.columeName as string)) {
                    return false;
                }
            }
        }

        if (!isGsiIndex(keyInfo)) {
            this.preCheck(keyInfo, orConditions);
        }

        if (!isGsiIndex(keyInfo)) {
            // 理论上 combineHash Key 应该要全部有，但是这里检查 gsi 主键，会影响到其他逻辑，所以只检查非 gsi 的主键。只把原始主键排除出去即可。
            const hasAllCombineHashKey = keyInfo.combineHashKey.every((key) => {
                return orConditions.some((andConditions) => {
                    return andConditions.some((condition) => {
                        return condition.columeName === key;
                    });
                });
            });
            return hasAllCombineHashKey;
        }
        return true;
    }

    // 将普通字段转换成 DynamoDB 类型
    // DynamoDB 不是简单的 key value，比较烦。另外，多主键的 table 要合并出一个主键 key 塞进去
    // 反过来可以看 transformToModelObject
    // eslint-disable-next-line max-len
    public transformModelToDynamoItem(
        target: MODEL,
    ): { [key: string]: AttributeValue } {
        const result: { [key: string]: AttributeValue } = {};
        for (const key in target) {
            const struct = this._columns[key] || this._keys[key];
            if (!struct) {
                // 保持和旧的报错一致
                throw new Error(`unexpect colume name ${key}`);
            }
            result[key] = tableStoreValueToDynamoAttr(target[key], struct);
        }

        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { indexes, ...primaryKeyInfo } = this.tableInfo;

        // TODO: 非文字类型的主键，可能要确认下，是否会主动转换为正确的字符串
        if (primaryKeyInfo.combineHashKey.length > 1) {
            const pk = [];
            for (const value of primaryKeyInfo.combineHashKey) {
                pk.push(target[value]);
            }
            result[primaryKeyInfo.hashKey] = {
                S: pk.join(splitKey),
            };
        }

        if (primaryKeyInfo.combineHashKey.length > 3) {
            const pks = this.combineHashKey.splice(0, 2);
            const pkValue = [];
            for (const value of pks) {
                pkValue.push(target[value]);
            }
            result[pks.join(splitKey)] = {
                S: pkValue.join(splitKey),
            };
        }

        for (const primaryKey in this._keys) {
            const keyStruct = this._keys[primaryKey]!;
            const value = target[primaryKey as keyof MODEL];
            const dbValue = keyStruct.toTableStoreValue(value as any);

            if (dbValue === null) {
                throw new Error(`lost primary key ${primaryKey}`);
            }
        }
        return result;
    }

    public get combineHashKey(): string[] {
        return this.tableInfo.combineHashKey as string[];
    }

    public get hashKey(): string {
        return this.tableInfo.hashKey as string;
    }

    public get rangeKey(): string | undefined {
        return this.tableInfo.rangeKey as string;
    }

    public get patchableKeys(): { readonly [K in keyof MODEL]: boolean } {
        return this._patchableKeys;
    }

    // 为了适配 连第一个主键（甚至只有一个主键）扫描的情况。实际可能剩下的主键是指定的，但是不继续做优化，统一直接 scan
    public get scanKeyInfo(): DynamoKeyInfo<MODEL> {
        return {
            tableName: this.tableInfo.tableName,
            hashKey: "",
            rangeKey: this.tableInfo.rangeKey,
            combineHashKey: this.tableInfo.combineHashKey,
            priority: Number.MAX_SAFE_INTEGER - 1,
            valueNode: this.tableInfo.valueNode,
        };
    }

}
