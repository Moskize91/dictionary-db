import type {
    AttributeValue,
    GetItemCommandOutput,
    QueryCommandInput,
    QueryCommandOutput,
    ScanCommandInput,
} from "@aws-sdk/client-dynamodb";
import type { DynamoTable } from "./DynamoTable";
import type { TableStoreTypeNode } from "../TableStoreType";

type DynamoOutputItem = Record<string, AttributeValue>;

export function transformToModelObject<MODEL extends { [key: string]: any }>(
    table: DynamoTable<MODEL>,
    output: { Item: DynamoOutputItem } | null,
): MODEL | null {
    if (output === null) {
        return null;
    }
    const item = output.Item;
    const result: { [key: string]: any } = {};
    for (const [key, keyStruct] of Object.entries(table.keys)) {
        const value = valueFromDynamoAttr(item[key], keyStruct!);
        result[key] = keyStruct.fromTableStoreValue(value);
    }
    for (const [column, struct] of Object.entries(table.columes)) {
        const value = valueFromDynamoAttr(item[column], struct!);
        result[column] = struct.fromTableStoreValue(value);
    }
    return result as MODEL;
}

export function transformToModelArray<MODEL extends { [key: string]: any }>(
    table: DynamoTable<MODEL>,
    output: { Items: DynamoOutputItem[] } | null,
): MODEL[] {
    if (output === null) {
        return [];
    }
    return (output.Items.map((item) => {
        const result: { [key: string]: any } = {};
        for (const [key, keyStruct] of Object.entries(table.keys)) {
            const value = valueFromDynamoAttr(item[key], keyStruct!);
            result[key] = keyStruct.fromTableStoreValue(value);
        }
        for (const [column, struct] of Object.entries(table.columes)) {
            const value = valueFromDynamoAttr(item[column], struct!);
            result[column] = struct.fromTableStoreValue(value);
        }
        return result;
    }) as MODEL[]);
}

export function removeOptionType<T extends { [key: string]: any }>(value: T | null | undefined): value is T {
    return value !== null && value !== undefined;
}

export function hasItemOutput(output: GetItemCommandOutput): output is { Item: DynamoOutputItem } & GetItemCommandOutput {
    return output.Item !== undefined;
}

export function hasItemsOutput(output: QueryCommandOutput): output is { Items: DynamoOutputItem[] } & QueryCommandOutput {
    return output.Items !== undefined;
}

export function isQueryCommandInput(input: QueryCommandInput | ScanCommandInput): input is QueryCommandInput {
    return (input as QueryCommandInput).KeyConditionExpression !== undefined;
}

// ????????????????????? float,integer,enum ?????????????????????????????? N???????????? DynamoDB ?????????????????????????????? String ????????????????????? Number ???????????????????????????
// eslint-disable-next-line max-len
export function valueFromDynamoAttr<T extends any>(value: AttributeValue | undefined, struct: TableStoreTypeNode<T>): T {
    if (value === undefined) {
        return null as any;
    }
    switch (struct.type) {
        case "boolean":
        { return value.BOOL as any; }
        case "string":
        { return value.S as any; }
        case "float":
        { return Number(value.N) as any; }
        case "integer":
        { return Number(value.N) as any; }
        case "buffer":
        { return value.B as any; }
        case "date":
        { return value.S as any; }
        case "enums":
        // FIXME: ??????????????? N?????????????????????????????? S?????? workaround????????????
        { return Number(value.N || value.S) as any; }
        case "booleanOptional":
        {
            if (value.BOOL === undefined) {
                return null as any;
            }
            return value.BOOL as any;
        }
        case "stringOptional":
        {
            if (value.S === undefined) {
                return null as any;
            }
            return value.S as any;
        }
        case "bufferOptional":
        {
            if (value.B === undefined) {
                return null as any;
            }
            return value.B as any;
        }
        case "dateOptional":
        {
            if (value.S === undefined) {
                return null as any;
            }
            return value.S as any;
        }
        case "enumsOptional":
        {
            if (value.N === undefined && value.S === undefined) {
                return null as any;
            }
            return Number(value.N || value.S) as any;
        }
        case "floatOptional":
        case "integerOptional":
        case "floatDefaultValue":
        case "integerDefaultValue":
        {
            if (value.N === undefined) {
                return null as any;
            }
            return Number(value.N) as any;
        }
        default:
        {
            // eslint-disable-next-line max-len
            return value.B || value.BOOL || value.L || value.M || value.N || value.NULL || value.S || value.SS || value.NS || value.BS || null as any;
        }
    }
}

// value ????????? null????????????????????? DynamoDB ???????????? null???????????????????????????dynamo ??? attribute ???????????????????????? { NULL: true }???primary keys ????????? null???
// ???????????? model ?????????????????? tableStoreValueToDynamoAttr ???????????? dynamo ??????
export function tableStoreValueToDynamoType(value: any): "S" | "N" | "B" | "BOOL" | "NULL" {
    if (value === undefined) {
        return "NULL";
    } else if (typeof value === "string") {
        return "S";
        // tablestore ??? long ????????? ??? toNumber ???????????????????????????????????????
    } else if (typeof value === "number" || value.toNumber) {
        return "N";
    } else if (typeof value === "boolean") {
        return "BOOL";
    } else if (value instanceof Buffer) {
        return "B";
    }
    return "S";
}

export function tableStoreValueToDynamoAttr(value: any, keyStruct: TableStoreTypeNode<any>): AttributeValue {
    const type = tableStoreValueToDynamoType(value);
    // ???????????? tablestore ????????????????????????????????? dynamo ???????????????
    let primaryValue = keyStruct.toTableStoreValue(value);
    // tablestore ??????????????????????????? tablestore.Long??????????????? toNumber ??????????????????
    if (primaryValue && primaryValue.toNumber) {
        primaryValue = primaryValue.toNumber();
    }
    switch (type) {
        case "S":
        { return { S: primaryValue.toString() }; }
        case "N":
        { return { N: primaryValue.toString() }; }
        case "BOOL":
        { return { BOOL: !!primaryValue }; }
        case "B":
        { return { B: primaryValue }; }
        case "NULL":
        { return { NULL: true }; }
    }
    // for eslint
    return { S: primaryValue.toString() };
}
