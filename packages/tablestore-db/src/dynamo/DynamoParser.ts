import type { Condition, Conditions } from "dictionary-db";
import type {
    AttributeValue,
    DeleteItemCommandInput,
    GetItemCommandInput,
    PutItemCommandInput,
    QueryCommandInput,
    UpdateItemCommandInput,
} from "@aws-sdk/client-dynamodb";
import { conditionsToString } from "../ConditionPrinter";
import { tableStoreValueToDynamoAttr, tableStoreValueToDynamoType } from "./DynamoTransfer";
import type { TableStoreTypeNode } from "../TableStoreType";
import type { DynamoKeyInfo, DynamoTable } from "./DynamoTable";
import { isGsiIndex, splitKey } from "./DynamoTable";

type ColumnAction = {
    PUT: {
        [key: string]: any;
    },
    DELETE: string[],
};

export function parseColumns<MODEL extends { [key: string]: any }>(
    patchableKeys: { readonly [K in keyof MODEL]: boolean },
    columns: Readonly<{ [K in keyof MODEL]?: TableStoreTypeNode<MODEL[K]> }>,
    { conditions, includesAll }: Conditions<MODEL>, target: Partial<MODEL>,
): ColumnAction {
    if (includesAll) {
        throw new Error("includesAll is not supported in DynamoDB");
    }
    const conditionsMap: { [primaryKey: string]: Condition<MODEL, keyof MODEL> } = {};

    for (const condition of conditions[0]!) {
        conditionsMap[condition.columeName as string] = condition;
    }

    const putColumns: { [key: string]: any } = {};
    const removeColumns: any[] = [];

    for (const key in target) {
        const condition = conditionsMap[key];
        const value = target[key];

        if (!patchableKeys[key] && condition && condition.value !== value) {
            throw new Error(`cannot change primary key "${key}"`);
        }
        const struct = columns[key];
        if (struct) {
            const dbValue = struct.toTableStoreValue(value as any);
            if (dbValue == null) {
                removeColumns.push(key);
            } else if (dbValue.toNumber) {
                // TODO: 当 long 类型时，阿里云用 buffer 替代了原始的 long，如果直接放进去，后续类型判断，会回落成字符串，所以提前转换数据类型
                putColumns[key] = dbValue.toNumber();
            } else {
                putColumns[key] = dbValue;
            }
        }
    }
    const updateOfAttributeColumns: ColumnAction = {} as any;
    if (Object.keys(putColumns).length > 0) {
        updateOfAttributeColumns.PUT = putColumns;
    }
    if (removeColumns.length > 0) {
        updateOfAttributeColumns.DELETE = removeColumns;
    }
    return updateOfAttributeColumns;
}

// 参考 parseConditionsToPrimaryKey 单行查询
// eslint-disable-next-line max-len
export function parseConditionsToGetCommand<MODEL extends { [key: string]: any }>(description: DynamoKeyInfo<MODEL>, { conditions, includesAll }: Conditions<MODEL>): GetItemCommandInput {
    if (includesAll) {
        throw new Error("cannot includes all");
    }
    if (conditions.length > 1) {
        throw new Error("tablestore not supported or condition");
    }
    const andConditions = conditions[0] || [];
    const primaryKeys: any = {};

    for (const primaryKey in description.valueNode) {
        const condition = andConditions.find((c) => c.columeName === primaryKey);
        if (!condition) {
            throw new Error(`lost primary key ${JSON.stringify(primaryKey)}`);
        }
        if (condition.sign !== "=") {
            throw new Error(`expect "=", invalid sign of ${JSON.stringify(primaryKey)}: ${condition.sign}`);
        }
        const struct = description.valueNode[primaryKey as keyof MODEL]!;
        const value = struct.toTableStoreValue(condition.value);

        primaryKeys[primaryKey] = value;
    }

    if (andConditions.length !== Object.keys(primaryKeys).length) {
        throw new Error(`invalid condition columes list ${conditionsToString(conditions)}`);
    }

    if (Object.keys(primaryKeys).length === 1) {
        return {
            TableName: description.tableName,
            Key: {
                [description.hashKey]: {
                    S: primaryKeys[description.hashKey],
                },
            },
        };
    } else if (Object.keys(primaryKeys).length === 2) {
        const hashType = tableStoreValueToDynamoType(primaryKeys[description.hashKey]);
        const rangeType = tableStoreValueToDynamoType(primaryKeys[description.rangeKey]);
        return {
            TableName: description.tableName,
            Key: {
                [description.hashKey]: {
                    [hashType]: primaryKeys[description.hashKey],
                },
                [description.rangeKey!]: {
                    [rangeType]: `${primaryKeys[description.rangeKey!]}`,
                },
            } as any,
        };
    } else {

        const { combineHashKey, hashKey, rangeKey } = description;
        if (combineHashKey.join(splitKey) !== hashKey) {
            throw new Error(`invalid combineHashKey ${combineHashKey.join(splitKey)}, except ${hashKey}`);
        }
        const pkValue = [];
        for (const key of combineHashKey) {
            if (!(key in primaryKeys)) {
                throw new Error(`lost primary key ${JSON.stringify(key)}`);
            }
            // TODO: 需要考虑 buffer to string，实际表结构中没有 buffer，暂时不考虑
            pkValue.push(primaryKeys[key]);
        }
        const rangeType = tableStoreValueToDynamoType(primaryKeys[rangeKey!]);
        const rangeValue = `${primaryKeys[rangeKey!]}`;

        return {
            TableName: description.tableName,
            Key: {
                [hashKey]: {
                    S: pkValue.join(splitKey),
                },
                [rangeKey!]: {
                    [rangeType]: rangeValue,
                } as any,
            },
        };
    }

}

function generateConditionExpression<MODEL extends { [key: string]: any }>(
    key: string,
    ref: string,
    struct: TableStoreTypeNode<MODEL[keyof MODEL]>,
    keyConditions: Condition<MODEL, keyof MODEL>[],
) : {
        conditionExpression: string;
        names: QueryCommandInput["ExpressionAttributeNames"];
        values: QueryCommandInput["ExpressionAttributeValues"];
    } {

    const q: {
        conditionExpression: string;
        names: QueryCommandInput["ExpressionAttributeNames"];
        values: QueryCommandInput["ExpressionAttributeValues"];
    } = {
        conditionExpression: "",
        names: {},
        values: {},
    };
    let start: any;
    let end: any;

    let bigThan: string | null = null;
    let smallThan: string | null = null;
    for (const { sign, value } of keyConditions) {
        switch (sign) {
            case "=": {
                if (start || end) {
                    throw new Error(`conflicting condition combination ${key} for ${sign}`);
                }
                start = struct.toTableStoreValue(value);
                end = start;
                break;
            }
            case ">": {
                if (start || bigThan) {
                    throw new Error(`conflicting condition combination ${key} for ${sign}`);
                }
                start = struct.toTableStoreValue(value);
                bigThan = sign;
                break;
            }
            case ">=": {
                if (start || bigThan) {
                    throw new Error(`conflicting condition combination ${key} for ${sign}`);
                }
                start = struct.toTableStoreValue(value);
                bigThan = sign;
                break;
            }
            case "<": {
                if (end || smallThan) {
                    throw new Error(`conflicting condition combination ${key!} for ${sign}`);
                }
                end = struct.toTableStoreValue(value);
                smallThan = sign;
                break;
            }
            case "<=": {
                if (end || smallThan) {
                    throw new Error(`conflicting condition combination ${key!} for ${sign}`);
                }
                end = struct.toTableStoreValue(value);
                smallThan = sign;
                break;
            }
            default: {
                throw new Error("dirty data, unsupported index sign");
            }
        }
    }
    let append = "";
    if (start && end && start !== end) {
        append = `#${ref} ${bigThan} :${ref}start AND #${ref} ${smallThan} :${ref}end`;
        q.names![`#${ref}`] = key;
        q.values![`:${ref}start`] = tableStoreValueToDynamoAttr(start, struct);
        q.values![`:${ref}end`] = tableStoreValueToDynamoAttr(end, struct);
    } else if (start && end) {
        append = `#${ref} = :${ref}start`;
        q.names![`#${ref}`] = key;
        q.values![`:${ref}start`] = tableStoreValueToDynamoAttr(start, struct);
    } else if (start) {
        append = `#${ref} ${bigThan} :${ref}start`;
        q.names![`#${ref}`] = key;
        q.values![`:${ref}start`] = tableStoreValueToDynamoAttr(start, struct);
    } else if (end) {
        append = `#${ref} ${smallThan} :${ref}end`;
        q.names![`#${ref}`] = key;
        q.values![`:${ref}end`] = tableStoreValueToDynamoAttr(end, struct);
    }
    q.conditionExpression = append;
    return q;
}

// 参考 parseConditionsToRange， parseConditionsToPrimaryKey ，后者需要把 onlyEqual 设置为 true。
// eslint-disable-next-line max-len
export function parseConditionsToRangeCommand<MODEL extends { [key: string]: any }>(keyInfo: DynamoKeyInfo<MODEL>, { conditions, includesAll }: Conditions<MODEL>, onlyEqual: boolean = false): QueryCommandInput {
    if (conditions.length > 1) {
        throw new Error("not supported or condition");
    }

    if (includesAll) {
        return {
            TableName: keyInfo.tableName,
        };
    }

    const andConditions = conditions[0];
    const sortedConditions: { [primaryKey: string]: Condition<MODEL, keyof MODEL>[] } = {};
    let sortedConditionsCount = 0;

    if (andConditions) {
        for (const primaryKey in keyInfo.valueNode) {
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
            // key 只需要取到最前面一部分就行，后面的可以不取，即完全满足
            if (!keyConditions) {
                break;
            }
            sortedConditions[primaryKey] = keyConditions;
        }
        if (sortedConditionsCount !== andConditions.length) {
            // 保持和旧代码一样的文字内容
            throw new Error(`invalid condition columes list: ${conditionsToString(conditions)}`);
        }

    }

    const { hashKey } = keyInfo;
    let q : QueryCommandInput;
    //  模拟 tablestore 单行查询时，业务上必须都是等于。这里需要模拟同样的行为。
    if (onlyEqual) {
        for (const primaryKey in keyInfo.valueNode) {
            const condition = andConditions.find((c) => c.columeName === primaryKey);
            if (!condition) {
                throw new Error(`lost primary key ${JSON.stringify(primaryKey)}`);
            }
            if (condition.sign !== "=") {
                throw new Error(`expect "=", invalid sign of ${JSON.stringify(primaryKey)}: ${condition.sign}`);
            }
        }
    }

    // hash key, 说明这个时候，请求在遍历第一个主键。
    // 没有任何一个固定值可以做 gsi 的 pk，只能 scan 了，然后用 filter 做过滤。
    if (hashKey === "") {
        q = {
            TableName: keyInfo.tableName,
        };
        const alpha = Array.from(Array(26)).map((e, i) => i + 65);
        const alphabet = alpha.map((x) => String.fromCharCode(x));
        for (const key in keyInfo.valueNode) {
            const struct = keyInfo.valueNode[key as keyof MODEL]!;
            const keyConditions = sortedConditions[key];
            // conditions 可能不包含所有 key
            if (keyConditions) {
                const { conditionExpression, names, values } = generateConditionExpression(key, alphabet.pop()!, struct, keyConditions);
                q.ExpressionAttributeNames = { ...q.ExpressionAttributeNames, ...names };
                q.ExpressionAttributeValues = { ...q.ExpressionAttributeValues, ...values };
                q.FilterExpression = q.FilterExpression ? `${q.FilterExpression} AND ${conditionExpression}` : conditionExpression;
            }
        }

        return q;
    } else if (hashKey.split(splitKey).length === 1) {
        const hashCondition = sortedConditions[hashKey];
        if (!hashCondition) {
            throw new Error(`lost primary key ${JSON.stringify(hashKey)}`);
        }
        const condition = hashCondition[0];
        if (condition.sign !== "=") {
            throw new Error(`expect "=", invalid sign of ${JSON.stringify(hashKey)}: ${condition.sign}`);
        }
        const struct = keyInfo.valueNode[hashKey as keyof MODEL]!;
        const value = struct.toTableStoreValue(condition.value);
        const valueType = tableStoreValueToDynamoType(value);
        q = {
            TableName: keyInfo.tableName,
            KeyConditionExpression: "#pk = :pk",
            ExpressionAttributeValues: {
                ":pk": {
                    [valueType]: value,
                } as AttributeValue,
            },
            ExpressionAttributeNames: {
                "#pk": hashKey,
            },
        };
    } else {
        const pks = hashKey.split(splitKey);
        const pkValue = [];
        for (const pk of pks) {
            if (!(pk in sortedConditions)) {
                throw new Error(`lost primary key ${JSON.stringify(pk)}`);
            }
            const condition = sortedConditions[pk][0];
            if (condition.sign !== "=") {
                throw new Error(`expect "=", invalid sign of ${JSON.stringify(pk)}: ${condition.sign}`);
            }
            const struct = keyInfo.valueNode[pk as keyof MODEL]!;
            const value = struct.toTableStoreValue(condition.value);
            pkValue.push(value);
        }
        q = {
            TableName: keyInfo.tableName,
            KeyConditionExpression: "#pk = :pk",
            ExpressionAttributeValues: {
                ":pk": {
                    S: pkValue.join(splitKey),
                } as AttributeValue,
            },
            ExpressionAttributeNames: {
                "#pk": hashKey,
            },
        };
    }
    sortedConditionsCount -= 1;

    if (keyInfo.rangeKey) {
        const rangeKey: string = keyInfo.rangeKey as string;
        const struct = keyInfo.valueNode[keyInfo.rangeKey]!;
        const keyConditions = sortedConditions[rangeKey];
        if (keyConditions) {
            const { conditionExpression, names, values } = generateConditionExpression(rangeKey, rangeKey, struct, keyConditions);
            q.ExpressionAttributeNames = { ...q.ExpressionAttributeNames, ...names };
            q.ExpressionAttributeValues = { ...q.ExpressionAttributeValues, ...values };
            q.KeyConditionExpression = `${q.KeyConditionExpression} AND ${conditionExpression}`;
        }
        sortedConditionsCount -= 1;
    }

    // combineHash key without hash key
    const restPrimaryKeys = Object.keys(sortedConditions).filter((k) => !hashKey.includes(k) && k !== keyInfo.rangeKey);
    if (restPrimaryKeys.length > 0) {
        const alpha = Array.from(Array(26)).map((e, i) => i + 65);
        const alphabet = alpha.map((x) => String.fromCharCode(x));
        for (const key of restPrimaryKeys) {
            const struct = keyInfo.valueNode[key as keyof MODEL]!;
            const keyConditions = sortedConditions[key];
            const { conditionExpression, names, values } = generateConditionExpression(key, alphabet.pop()!, struct, keyConditions);
            q.ExpressionAttributeNames = { ...q.ExpressionAttributeNames, ...names };
            q.ExpressionAttributeValues = { ...q.ExpressionAttributeValues, ...values };
            q.FilterExpression = q.FilterExpression ? `${q.FilterExpression} AND ${conditionExpression}` : conditionExpression;
        }
    }

    if (isGsiIndex(keyInfo)) {
        q.IndexName = keyInfo.indexName;
    }

    return q;
}

export function parseTargetToPutCommand<MODEL extends { [key: string]: any }>(
    input: MODEL,
    table: DynamoTable<MODEL>,
    isOverride: boolean,
): PutItemCommandInput {
    const putCommand: PutItemCommandInput = {
        TableName: table.name,
        Item: table.transformModelToDynamoItem(input),
    };
    if (!isOverride) {
        putCommand.ConditionExpression = "attribute_not_exists(#c)";
        putCommand.ExpressionAttributeNames = { "#c": table.hashKey };
        if (table.rangeKey) {
            putCommand.ExpressionAttributeNames["#r"] = table.rangeKey;
            putCommand.ConditionExpression += " AND attribute_not_exists(#r)";
        }
    }
    return putCommand;
}

export function parseUpdateCommand<MODEL extends { [key: string]: any }>(
    itemInput: GetItemCommandInput,
    actionColumns: ColumnAction,
    isOverride: boolean,
    tableNode: DynamoTable<MODEL>,
): UpdateItemCommandInput {
    let updateExpression = "";
    const attributeNames: Record<string, string> = {};
    const attributeValues: Record<string, AttributeValue> = {};
    for (const type in actionColumns) {
        if (type === "PUT") {
            updateExpression += updateExpression === "" ? "SET " : " SET ";
            const updateExpressionList: string[] = [];
            for (const column in actionColumns[type]) {
                updateExpressionList.push(`#${column} = :${column}`);
                const value = actionColumns[type][column];
                attributeNames[`#${column}`] = column;
                const attrType = tableStoreValueToDynamoType(value);
                attributeValues[`:${column}`] = {
                    [attrType]: attrType === "B" ? value : value.toString(),
                } as any as AttributeValue;
            }
            updateExpression += updateExpressionList.join(", ");
        } else if (type === "DELETE") {
            updateExpression += updateExpression === "" ? "REMOVE " : " REMOVE ";
            const removeExpressionList = [];
            for (const column of actionColumns[type]) {
                attributeNames[`#${column}`] = column;
                removeExpressionList.push(`#${column}`);
            }
            updateExpression += removeExpressionList.join(", ");
        }
    }

    const updateInput: UpdateItemCommandInput = itemInput;

    updateInput.UpdateExpression = updateExpression;
    updateInput.ExpressionAttributeNames = attributeNames;
    updateInput.ExpressionAttributeValues = attributeValues;
    // const expectation = isOverride ? TableStore.RowExistenceExpectation.IGNORE : TableStore.RowExistenceExpectation.EXPECT_EXIST;
    // 根据 tablestore 代码，isOverride 如果为 false，则要求存在数据，有点奇怪，但是保持原有逻辑不动
    if (!isOverride) {
        updateInput.ConditionExpression = "attribute_exists(#c)";
        updateInput.ExpressionAttributeNames = { ...updateInput.ExpressionAttributeNames, "#c": tableNode.hashKey };
        if (tableNode.rangeKey) {
            updateInput.ExpressionAttributeNames["#r"] = tableNode.rangeKey;
            updateInput.ConditionExpression += " AND attribute_exists(#r)";
        }
    }
    return updateInput;
}

export function addDeleteCommandCondition<MODEL extends { [key: string]: any }>(
    itemInput: GetItemCommandInput,
    tableNode: DynamoTable<MODEL>,
): DeleteItemCommandInput {
    const deleteInput: DeleteItemCommandInput = itemInput;
    deleteInput.ConditionExpression = "attribute_exists(#c)";
    deleteInput.ExpressionAttributeNames = { "#c": tableNode.hashKey };
    if (tableNode.rangeKey) {
        deleteInput.ExpressionAttributeNames["#r"] = tableNode.rangeKey;
        deleteInput.ConditionExpression += " AND attribute_exists(#r)";
    }
    return deleteInput;
}
