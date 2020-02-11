import type { ModelDefinition } from "./Model";

export interface DatabaseAdapter<MODELS extends Object> {
    getModelDefinitions(): { readonly [K in keyof MODELS]: ModelDefinition<MODELS[K]> };

    exits?<M extends keyof MODELS>(model: M, description: GetDescription<MODELS[M]>): Promise<boolean>;
    count?<M extends keyof MODELS>(model: M, description: GetDescription<MODELS[M]>): Promise<number>;
    get?<M extends keyof MODELS>(model: M, description: GetDescription<MODELS[M]>): Promise<MODELS[M] | null>;
    getAll?<M extends keyof MODELS>(model: M, description: GetDescription<MODELS[M]>): Promise<ReadonlyArray<MODELS[M]>>;
    getWithSlices<M extends keyof MODELS>(
        model: M, description: GetDescription<MODELS[M]>,
        handler: (slices: ReadonlyArray<MODELS[M]>, stop: () => void) => Promise<void>): Promise<number>;

    create<M extends keyof MODELS>(model: M, target: MODELS[M], isOverride: boolean): Promise<boolean>;
    set<M extends keyof MODELS>(
        model: M, conditions: Conditions<MODELS[M]>,
        target: MODELS[M], isOverride: boolean): Promise<boolean>;

    update<M extends keyof MODELS>(
        model: M, conditions: Conditions<MODELS[M]>,
        target: Partial<MODELS[M]>, isOverride: boolean): Promise<boolean>;

    delete?<M extends keyof MODELS>(model: M, conditions: Conditions<MODELS[M]>): Promise<boolean>;
    deleteAll<M extends keyof MODELS>(model: M, conditions: Conditions<MODELS[M]>, limitCount?: number): Promise<number>;
}

export type Conditions<T extends { [key: string]: any }> = {
    readonly includesAll: boolean;
    readonly conditions: ReadonlyArray<ReadonlyArray<Condition<T, keyof T>>>;
};

export type Condition<T extends { [key: string]: any }, K extends keyof T> = {
    readonly columeName: K;
    readonly sign: "=" | "!=" | ">" | "<" | ">=" | "<=";
    readonly value: T[K];
};

export type GetDescription<T extends { [key: string]: any }> = {
    readonly conditions: Conditions<T>;
    readonly limitCount?: number;
    readonly slicesCount?: number;
    readonly isAscending?: boolean;
};
