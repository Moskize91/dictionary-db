export type ModelDefinition<T extends { [key: string]: any }> = {
    readonly [K in keyof T]: PropertyDefinition<T[K]>;
};

export type PropertyDefinition<V> = {
    readonly isConditionable: boolean;
    readonly isValid: (value: V) => boolean;
};

export interface Model<T extends { [key: string]: any }> {
    readonly name: string;
    readonly get: ConditionBuilder<GetBuilder<T>, T>;
    readonly set: ConditionBuilder<SetBuilder<T>, T>;
    readonly post: Poster<T>;
}

export interface GetBuilder<T extends { [key: string]: any }> {
    readonly and: ConditionBuilder<this, T>;
    readonly or: ConditionBuilder<this, T>;

    ascending(): this;
    descending(): this;
    limit(count: number): this;
    slices(count: number): this;
    key(key: T["key"]): this;

    result(): Promise<T | null>;
    results(): Promise<ReadonlyArray<T>>;
    resultSlices(handler: (slices: ReadonlyArray<T>, stop: () => void) => Promise<void>): Promise<number>;

    value(): Promise<T["value"] | null>;
    values(): Promise<ReadonlyArray<T["value"]>>;
    valueSlices(handler: (slices: ReadonlyArray<T["value"]>, stop: () => void) => Promise<void>): Promise<number>;

    exits(): Promise<boolean>;
    count(): Promise<number>;
}

export interface SetBuilder<T extends { [key: string]: any }> {
    readonly and: ConditionBuilder<this, T>;
    readonly or: ConditionBuilder<this, T>;

    override(): this;
    put(value: T): Promise<boolean>;
    patch(value: Partial<T>): Promise<boolean>;
    delete(): Promise<boolean>;
    deleteAll(): Promise<number>;
}

export interface ConditionBuilder<B, T extends { [key: string]: any }> {
    all(): B;
    colume<K extends keyof T>(name: K): ConditionFiller<B, T[K], T>;
}

export interface Poster<T> {
    override(value: T): Promise<boolean>;
    (value: T): Promise<boolean>;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export interface ConditionFiller<B, V, T extends { [key: string]: any }> {
    equals(value: V): B;
    notEqualTo(value: V): B;
    greaterThan(value: V): B;
    greaterOrEqualsThan(value: V): B;
    lessThan(value: V): B;
    lessOrEqualsThan(value: V): B;
}
