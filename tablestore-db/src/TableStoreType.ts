import TableStore from "tablestore";

export type TableStoreModel<T extends { [key: string]: any }> = {
    readonly keys: {
        readonly [K in keyof T]?: TableStoreTypeNode<T[K]>;
    };
    readonly columes: {
        readonly [K in keyof T]?: TableStoreTypeNode<T[K]>;
    };
    readonly indexes?: {
        readonly [tableName: string]: ReadonlyArray<keyof T>;
    };
};

export type TableStoreTypeNode<V> = {
    readonly isValid: (value: V) => boolean;
    readonly toTableStoreValue: (value: V) => any;
    readonly fromTableStoreValue: (value: any) => V;
};

export const TableStoreType: TableStoreTypeDefinition = createTableStoreType();

export type TableStoreTypeDefinition = {
    readonly boolean: TableStoreTypeNode<boolean>;
    readonly string: TableStoreTypeNode<string>;
    readonly float: TableStoreTypeNode<number>;
    readonly integer: TableStoreTypeNode<number>;
    readonly buffer: TableStoreTypeNode<Buffer>;
    readonly date: TableStoreTypeNode<Date>;
    readonly enums: <E extends string>(enums: E[]) => TableStoreTypeNode<E>;

    readonly booleanOptional: TableStoreTypeNode<boolean | undefined>;
    readonly stringOptional: TableStoreTypeNode<string | undefined>;
    readonly floatOptional: TableStoreTypeNode<number | undefined>;
    readonly integerOptional: TableStoreTypeNode<number | undefined>;
    readonly bufferOptional: TableStoreTypeNode<Buffer | undefined>;
    readonly dateOptional: TableStoreTypeNode<Date | undefined>;
    readonly enumsOptional: <E extends string>(enums: E[]) => TableStoreTypeNode<E | undefined>;

    readonly booleanDefaultValue: (defaultValue: boolean) => TableStoreTypeNode<boolean>;
    readonly stringDefaultValue: (defaultValue: string) => TableStoreTypeNode<string>;
    readonly floatDefaultValue: (defaultValue: number) => TableStoreTypeNode<number>;
    readonly integerDefaultValue: (defaultValue: number) => TableStoreTypeNode<number>;
    readonly bufferDefaultValue: (defaultValue: Buffer) => TableStoreTypeNode<Buffer>;
    readonly dateDefaultValue: (defaultValue: Date) => TableStoreTypeNode<Date>;
    readonly enumsDefaultValue: <E extends string>(enums: E[], defaultValue: E) => TableStoreTypeNode<E>;
};

export function isTablestoreValueEquals(value1: any, value2: any): boolean {
    if (value1 === value2) {
        return true;
    } else if (isTableStoreLong(value1) && isTableStoreLong(value2)) {
        return value1.toString() === value2.toString();
    } else {
        return false;
    }
}

function isTableStoreLong(value: any): boolean {
    return (
        value !== null &&
        typeof value === "object" &&
        typeof value.toNumber === "function"
    );
}

function createTableStoreType(): TableStoreTypeDefinition {
    const types = {
        boolean: createJavascriptTypeNode<boolean>((value) => typeof value === "boolean"),
        string: createJavascriptTypeNode<string>((value) => typeof value === "string"),
        float: createJavascriptTypeNode<number>((value) => typeof value === "number"),
        buffer: createJavascriptTypeNode<Buffer>((value) => Buffer.isBuffer(value)),

        integer: Object.freeze({
            isValid: (value: number) => Number.isSafeInteger(value),
            toTableStoreValue: (value: number) => TableStore.Long.fromNumber(value),
            fromTableStoreValue: (value: any) => {
                if (typeof value === "number") {
                    return value;
                } else {
                    return value.toNumber();
                }
            },
        }),

        date: Object.freeze({
            isValid: (value: Date) => value instanceof Date,
            toTableStoreValue: (value: Date) => value.toISOString(),
            fromTableStoreValue: (value: any) => new Date(value),
        }),

        enums: <E extends string>(enums: E[]): TableStoreTypeNode<E> => {
            const enumList = Object.freeze([...enums]);
            return Object.freeze({
                isValid: (value) => {
                    if (typeof value !== "string") {
                        return false;
                    }
                    const index = enumList.indexOf(value);

                    if (index === -1) {
                        return false;
                    }
                    return true;
                },
                toTableStoreValue: (value) => {
                    const index = enumList.indexOf(value);

                    if (index === -1) {
                        throw new Error(`unrecognized enum "${value}"`);
                    }
                    return TableStore.Long.fromNumber(index);
                },
                fromTableStoreValue: (value) => {
                    let index: number;
                    if (typeof value === "number") {
                        index = value;
                    } else {
                        index = value.toNumber();
                    }
                    const enumValue = enumList[index];

                    if (enumValue === undefined) {
                        throw new Error(`unrecognized enum index ${value}`);
                    }
                    return enumValue;
                },
            });
        },
    };
    const enumType = types.enums;
    const wrappedTypes = {
        ...types,

        booleanOptional: wrapOptionalTypeNode(types.boolean),
        stringOptional: wrapOptionalTypeNode(types.string),
        floatOptional: wrapOptionalTypeNode(types.float),
        integerOptional: wrapOptionalTypeNode(types.integer),
        bufferOptional: wrapOptionalTypeNode(types.buffer),
        dateOptional: wrapOptionalTypeNode(types.date),

        enumsOptional: <E extends string>(
            enumsList: E[]): TableStoreTypeNode<E | undefined> => wrapOptionalTypeNode(types.enums(enumsList)),

        booleanDefaultValue: wrapDefaultValueTypeNode(types.boolean),
        stringDefaultValue: wrapDefaultValueTypeNode(types.string),
        floatDefaultValue: wrapDefaultValueTypeNode(types.float),
        integerDefaultValue: wrapDefaultValueTypeNode(types.integer),
        bufferDefaultValue: wrapDefaultValueTypeNode(types.buffer),
        dateDefaultValue: wrapDefaultValueTypeNode(types.date),
        enumsDefaultValue: <E extends string>(enumsList: E[], defaultValue: E): TableStoreTypeNode<E> => {
            const enumsType = enumType(enumsList);
            const enumsTypeWithDefaultValue = wrapDefaultValueTypeNode(enumsType);
            return enumsTypeWithDefaultValue(defaultValue);
        },
    };
    return Object.freeze(wrappedTypes);
}

function createJavascriptTypeNode<V>(isValid: (value: V) => boolean): TableStoreTypeNode<V> {
    return Object.freeze({
        isValid,
        toTableStoreValue: (value) => value,
        fromTableStoreValue: (value) => value,
    });
}

function wrapOptionalTypeNode<V>(typeNode: TableStoreTypeNode<V>): TableStoreTypeNode<V | undefined> {
    return Object.freeze({
        isValid: (value) => {
            if (value === undefined) {
                return true;
            } else {
                return typeNode.isValid(value);
            }
        },
        toTableStoreValue: (value) => {
            if (value === undefined || value === null) {
                return null;
            }
            return typeNode.toTableStoreValue(value);
        },
        fromTableStoreValue: (value) => {
            if (value === null) {
                return undefined;
            }
            return typeNode.fromTableStoreValue(value);
        },
    });
}

function wrapDefaultValueTypeNode<V>(typeNode: TableStoreTypeNode<V>): (defaultValue: V) => TableStoreTypeNode<V> {
    return (defaultValue) => Object.freeze({
        isValid: (value: V) => {
            if (value === undefined) {
                return true;
            } else {
                return typeNode.isValid(value);
            }
        },
        toTableStoreValue: (value: V) => {
            if (value === undefined || value === null) {
                return defaultValue;
            }
            return typeNode.toTableStoreValue(value);
        },
        fromTableStoreValue: (value: any) => {
            if (value === null) {
                return defaultValue;
            }
            return typeNode.fromTableStoreValue(value);
        },
    });
}
