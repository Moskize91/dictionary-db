import type { Conditions, Condition } from "netless-dictionary-db";
import type { TableStoreModel, TableStoreTypeNode } from "./TableStoreType";
import { conditionsToString } from "./ConditionPrinter";

export type KeysDescription<MODELS extends { [key: string]: any }> = {
    readonly name: string;
    readonly isIndex: boolean;
    readonly indexKeysCount: number;
    readonly patchableKeys: { readonly [K in keyof MODELS]: boolean };
    readonly combinedKeys: {
        readonly [K in keyof MODELS]?: TableStoreTypeNode<MODELS[K]>;
    };
    readonly indexKeys: {
        readonly [K in keyof MODELS]?: TableStoreTypeNode<MODELS[K]>;
    };
};

export class ModelNode<MODELS extends { [key: string]: any }> {

    private readonly selfKeys: KeysDescription<MODELS>;
    private readonly _keys: ReadonlyArray<KeysDescription<MODELS>>;
    private readonly _columes: { readonly [K in keyof MODELS]?: TableStoreTypeNode<MODELS[K]> };

    public constructor(name: string, model: TableStoreModel<MODELS>) {
        const patchableKeys = this.createPatchableKeys(model);

        this.selfKeys = Object.freeze({
            name,
            isIndex: false,
            patchableKeys,
            indexKeysCount: Object.keys(model.keys).length,
            combinedKeys: Object.freeze({ ...model.keys }),
            indexKeys: Object.freeze({ ...model.keys }),
        });
        const tables: KeysDescription<MODELS>[] = [this.selfKeys];

        if (model.indexes) {
            for (const indexName in model.indexes) {
                const indexPrimiaryKeys = model.indexes[indexName];
                const combinedKeys: { [K in keyof MODELS]?: TableStoreTypeNode<MODELS[K]> } = {};

                for (const key of indexPrimiaryKeys) {
                    combinedKeys[key] = model.keys[key] || model.columes[key];
                }
                for (const key in model.keys) {
                    if (!combinedKeys[key]) {
                        combinedKeys[key] = model.keys[key];
                    }
                }
                tables.push(Object.freeze({
                    name: indexName,
                    isIndex: true,
                    patchableKeys,
                    indexKeysCount: Object.keys(combinedKeys).length,
                    combinedKeys: Object.freeze(combinedKeys),
                    indexKeys: Object.freeze(combinedKeys),
                }));
            }
        }
        this._keys = Object.freeze(tables);
        this._columes = Object.freeze({ ...model.columes });
    }

    private createPatchableKeys(model: TableStoreModel<MODELS>): { readonly [K in keyof MODELS]: boolean } {
        const keys: { [K in keyof MODELS]?: boolean } = {};
        for (const key in model.columes) {
            keys[key] = true;
        }
        for (const key in model.keys) {
            keys[key] = false;
        }
        return Object.freeze(keys as { [K in keyof MODELS]: boolean });
    }

    public get columes(): { readonly [K in keyof MODELS]?: TableStoreTypeNode<MODELS[K]> } {
        return this._columes;
    }

    public get keys(): { readonly [K in keyof MODELS]?: TableStoreTypeNode<MODELS[K]> } {
        return this.selfKeys.combinedKeys;
    }

    public isIndexKey(key: keyof MODELS): boolean {
        for (const keysDescription of this._keys) {
            if (keysDescription.isIndex && key in keysDescription.indexKeys) {
                return true;
            }
        }
        return false;
    }

    public keysDescription({ conditions, includesAll }: Conditions<MODELS>): KeysDescription<MODELS> {
        if (includesAll) {
            return this.selfKeys;
        }
        let suitableKeysDescription: KeysDescription<MODELS> | null = null;
        let currentIndexKeysCount = Number.MAX_SAFE_INTEGER;

        for (const keysDescription of this._keys) {
            if (keysDescription.indexKeysCount < currentIndexKeysCount &&
                this.isCondtionsMatch(keysDescription, conditions)) {
                suitableKeysDescription = keysDescription;
                currentIndexKeysCount = keysDescription.indexKeysCount;
            }
        }
        if (!suitableKeysDescription) {
            throw new Error(`conditions cannot match any primiary keys or index keys: ${conditionsToString(conditions)}`);
        }
        return suitableKeysDescription;
    }

    private isCondtionsMatch({ indexKeys }: KeysDescription<MODELS>,
                             orConditions: ReadonlyArray<ReadonlyArray<Condition<MODELS, keyof MODELS>>>): boolean {
        for (const andConditions of orConditions) {
            for (const condition of andConditions) {
                if (!indexKeys[condition.columeName]) {
                    return false;
                }
            }
        }
        return true;
    }

}
