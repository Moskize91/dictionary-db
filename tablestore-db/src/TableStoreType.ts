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
    readonly type: keyof TableStoreTypeDefinition;
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

export type TableStoreModelDefinition<MODELS extends { [key: string]: any }> = {
    readonly [K in keyof MODELS]: TableStoreModel<MODELS[K]>;
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

type PrimaryType = "boolean" | "string" | "float" | "integer" | "buffer" | "date";

function createTableStoreType(): TableStoreTypeDefinition {
    const types: { [key in PrimaryType]: TableStoreTypeNode<any> } = {
        boolean: createJavascriptTypeNode<boolean, "boolean">((value) => typeof value === "boolean", "boolean"),
        string: createJavascriptTypeNode<string, "string">((value) => typeof value === "string", "string"),
        float: createJavascriptTypeNode<number, "float">((value) => typeof value === "number", "float"),
        buffer: createJavascriptTypeNode<Buffer, "buffer">((value) => Buffer.isBuffer(value), "buffer"),

        integer: Object.freeze({
            type: "integer",
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
            type: "date",
            isValid: (value: Date) => value instanceof Date,
            toTableStoreValue: (value: Date) => value.toISOString(),
            fromTableStoreValue: (value: any) => new Date(value),
        }),
    };

    const enums = <E extends string>(enums: E[]): TableStoreTypeNode<E> => {
        const enumList = Object.freeze([...enums]);
        return Object.freeze({
            type: "enums",
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
    };

    const enumType = enums;
    const wrappedTypes = {
        ...types,
        enums,

        booleanOptional: wrapOptionalTypeNode(types.boolean, "booleanOptional"),
        stringOptional: wrapOptionalTypeNode(types.string, "stringOptional"),
        floatOptional: wrapOptionalTypeNode(types.float, "floatOptional"),
        integerOptional: wrapOptionalTypeNode(types.integer, "integerOptional"),
        bufferOptional: wrapOptionalTypeNode(types.buffer, "bufferOptional"),
        dateOptional: wrapOptionalTypeNode(types.date, "dateOptional"),

        enumsOptional: <E extends string>(
            enumsList: E[]): TableStoreTypeNode<E | undefined> => wrapOptionalTypeNode(enums(enumsList), "enumsOptional"),

        booleanDefaultValue: wrapDefaultValueTypeNode(types.boolean, "booleanDefaultValue"),
        stringDefaultValue: wrapDefaultValueTypeNode(types.string, "stringDefaultValue"),
        floatDefaultValue: wrapDefaultValueTypeNode(types.float, "floatDefaultValue"),
        integerDefaultValue: wrapDefaultValueTypeNode(types.integer, "integerDefaultValue"),
        bufferDefaultValue: wrapDefaultValueTypeNode(types.buffer, "bufferDefaultValue"),
        dateDefaultValue: wrapDefaultValueTypeNode(types.date, "dateDefaultValue"),
        enumsDefaultValue: <E extends string>(enumsList: E[], defaultValue: E): TableStoreTypeNode<E> => {
            const enumsType = enumType(enumsList);
            const enumsTypeWithDefaultValue = wrapDefaultValueTypeNode(enumsType, "enumsDefaultValue");
            return enumsTypeWithDefaultValue(defaultValue);
        },
    };
    return Object.freeze(wrappedTypes);
}

// eslint-disable-next-line max-len
function createJavascriptTypeNode<V, T extends keyof TableStoreTypeDefinition>(isValid: (value: V) => boolean, type: T): TableStoreTypeNode<V> {
    return Object.freeze({
        type,
        isValid,
        toTableStoreValue: (value) => value,
        fromTableStoreValue: (value) => value,
    });
}

// eslint-disable-next-line max-len
function wrapOptionalTypeNode<V, T extends keyof TableStoreTypeDefinition>(typeNode: TableStoreTypeNode<V>, type: T): TableStoreTypeNode<V | undefined> {
    return Object.freeze({
        type,
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

// eslint-disable-next-line max-len
function wrapDefaultValueTypeNode<V, T extends keyof TableStoreTypeDefinition>(typeNode: TableStoreTypeNode<V>, type: T): (defaultValue: V) => TableStoreTypeNode<V> {
    return (defaultValue) => Object.freeze({
        type,
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
