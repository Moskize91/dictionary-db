import type { Model, ModelDefinition, GetBuilder, SetBuilder, Poster, ConditionBuilder } from "./Model";
import type { DatabaseAdapter } from "./DatabaseAdapter";
import { GetBuilderImplement } from "./GetBuilder";
import { SetBuilderImplement } from "./SetBuilder";

export class Database<MODELS extends Object> {

    private readonly models: { readonly [K in keyof MODELS]: Model<MODELS[K]> };

    public constructor(adtaper: DatabaseAdapter<MODELS>) {
        const models: { [K in keyof MODELS]?: Model<MODELS[K]> } = {};
        const modelDefinitions = adtaper.getModelDefinitions();

        for (const key in modelDefinitions) {
            models[key] = new ModelImplement(key, adtaper as any, modelDefinitions[key]);
        }
        this.models = Object.freeze(models) as { readonly [K in keyof MODELS]: Model<MODELS[K]> };
    }

    public model<K extends keyof MODELS>(modelName: K): Model<MODELS[K]> {
        const modelNode = this.models[modelName];
        if (!modelNode) {
            throw new Error(`cannot find model named ${JSON.stringify(modelName)}`);
        }
        return modelNode;
    }

}

export class ModelImplement<NAME extends string, T extends { [key: string]: any }> implements Model<T> {

    public readonly name: NAME;
    public readonly adtaper: DatabaseAdapter<{ [key in NAME]: T }>;
    public readonly definition: ModelDefinition<T>;
    public readonly conditionableKeys: ReadonlyArray<keyof T>;

    private readonly conditionableKeysMap: { readonly [K in keyof T]: boolean };

    public constructor(name: NAME, adtaper: DatabaseAdapter<{ [key in NAME]: T }>, definition: ModelDefinition<T>) {
        this.name = name;
        this.adtaper = adtaper;
        this.definition = definition;

        const conditionableKeysMap: { [K in keyof T]?: boolean } = {};
        const conditionableKeys: (keyof T)[] = [];

        for (const key in definition) {
            const { isConditionable } = definition[key];
            if (isConditionable) {
                conditionableKeys.push(key);
            }
            conditionableKeysMap[key] = isConditionable;
        }
        this.conditionableKeysMap = Object.freeze(conditionableKeysMap) as { readonly [K in keyof T]: boolean };
        this.conditionableKeys = Object.freeze(conditionableKeys);
    }

    public get post(): Poster<T> {
        const poster = (value: T): Promise<boolean> => this.handlePost(value, false);
        (poster as any).override = (value: T) => this.handlePost(value, true);
        return poster as Poster<T>;
    }

    public get get(): ConditionBuilder<GetBuilder<T>, T> {
        return new GetBuilderImplement(this).who();
    }

    public get set(): ConditionBuilder<SetBuilder<T>, T> {
        return new SetBuilderImplement(this).who();
    }

    public isConditionableKey(key: keyof T): boolean {
        return this.conditionableKeysMap[key];
    }

    private async handlePost(value: T, isOverride: boolean): Promise<boolean> {
        for (const key in this.definition) {
            if (!this.definition[key].isValid(value[key])) {
                throw new Error(`invalid value of ${JSON.stringify(key)}: ${value[key]}`);
            }
        }
        return this.adtaper.create(this.name, value, isOverride);
    }

}
