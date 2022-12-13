import type { GetBuilder, ConditionBuilder } from "./Model";
import type { ModelImplement } from "./Database";
import type { GetDescription } from "./DatabaseAdapter";
import { ConditionContext } from "./ConditionContext";

export class GetBuilderImplement<NAME extends string, T extends { [key: string]: any }> implements GetBuilder<T> {

    private readonly condition: ConditionContext<this, T> = new ConditionContext(this);

    private limitCount?: number = undefined;
    private slicesCount?: number = undefined;
    private isAscending?: boolean = undefined;

    public constructor(
        private readonly model: ModelImplement<NAME, T>,
    ) { }

    public ascending(): this {
        if (this.isAscending !== undefined) {
            throw new Error("did set order");
        }
        this.isAscending = true;
        return this;
    }

    public descending(): this {
        if (this.isAscending !== undefined) {
            throw new Error("did set order");
        }
        this.isAscending = false;
        return this;
    }

    public limit(count: number): this {
        if (this.limitCount !== undefined) {
            throw new Error("did set limit");
        }
        this.limitCount = count;
        return this;
    }

    public slices(count: number): this {
        if (this.slicesCount !== undefined) {
            throw new Error("did set slices");
        }
        this.slicesCount = count;
        return this;
    }

    public who(): ConditionBuilder<this, T> {
        return this.condition.createBuilder();
    }

    public get and(): ConditionBuilder<this, T> {
        return this.condition.createBuilder(true);
    }

    public get or(): ConditionBuilder<this, T> {
        return this.condition.createBuilder(false);
    }

    public key(key: T["key"]): this {
        return this.who().colume("key").equals(key);
    }

    public async value(): Promise<T["value"]> {
        const result = await this.result();
        return result && result.value;
    }

    public async values(): Promise<ReadonlyArray<T["value"]>> {
        return GetBuilderImplement.replaceResultsToValues(await this.results());
    }

    public valueSlices(handler: (slices: ReadonlyArray<T["value"]>, stop: () => void) => Promise<void>): Promise<number> {
        return this.resultSlices((results, stop) => handler(GetBuilderImplement.replaceResultsToValues(results), stop));
    }

    private static replaceResultsToValues<T extends { [key: string]: any }>(results: ReadonlyArray<T>): T["value"][] {
        const values: T["value"][] = [];
        for (const result of results) {
            values.push(result.value);
        }
        return values;
    }

    public async exits(): Promise<boolean> {
        const adapter = this.model.adtaper;
        if (adapter.exits) {
            const description = this.generateGetDescription();
            return await adapter.exits(this.model.name, description);
        } else {
            return await this.result() !== null;
        }
    }

    public async count(): Promise<number> {
        const adapter = this.model.adtaper;
        if (adapter.count) {
            const description = this.generateGetDescription();
            return await adapter.count(this.model.name, description);
        } else {
            return (await this.results()).length;
        }
    }

    public async result(): Promise<T | null> {
        const adapter = this.model.adtaper;
        const description = this.generateGetDescription();

        if (adapter.get) {
            return await adapter.get(this.model.name, description);

        } else if (adapter.getAll) {
            const results = await adapter.getAll(this.model.name, description);
            if (results.length > 0) {
                return results[0];
            } else {
                return null;
            }
        } else {
            let result: T | null = null;
            await adapter.getWithSlices(this.model.name, description, async (slices, stop) => {
                if (slices.length > 0) {
                    // eslint-disable-next-line prefer-destructuring
                    result = slices[0];
                }
                stop();
            });
            return result;
        }
    }

    public async results(): Promise<readonly T[]> {
        const adapter = this.model.adtaper;
        const description = this.generateGetDescription();

        if (adapter.getAll) {
            return await adapter.getAll(this.model.name, description);
        } else {
            const results: T[] = [];
            await adapter.getWithSlices(this.model.name, description, async (slices) => {
                results.push(...slices);
            });
            return results;
        }
    }

    public async resultSlices(handler: (slices: readonly T[], stop: () => void) => Promise<void>): Promise<number> {
        const adapter = this.model.adtaper;
        const description = this.generateGetDescription();

        return await adapter.getWithSlices(this.model.name, description, handler);
    }

    private generateGetDescription(): GetDescription<T> {
        const conditions = this.condition.generate(this.model.definition);
        return {
            conditions,
            limitCount: this.limitCount,
            slicesCount: this.slicesCount,
            isAscending: this.isAscending,
        };
    }

}
