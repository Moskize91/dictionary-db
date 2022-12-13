import type { SetBuilder, ConditionBuilder } from "./Model";
import type { ModelImplement } from "./Database";
import { ConditionContext } from "./ConditionContext";

export class SetBuilderImplement<NAME extends string, T extends { [key: string]: any }> implements SetBuilder<T> {

    private readonly condition: ConditionContext<this, T> = new ConditionContext(this);
    private limitCount?: number = undefined;
    private isOverride?: boolean = undefined;

    public constructor(
        private readonly model: ModelImplement<NAME, T>,
    ) { }

    public override(): this {
        if (this.isOverride !== undefined) {
            throw new Error("did set override");
        }
        this.isOverride = true;
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

    public async put(value: T): Promise<boolean> {
        if (this.limitCount !== undefined) {
            throw new Error("put cannot set limit");
        }
        const { definition } = this.model;
        const conditions = this.condition.generate(this.model.definition);

        for (const key in definition) {
            if (!definition[key].isValid(value[key])) {
                throw new Error(`invalid value of ${JSON.stringify(key)}: ${value[key]}`);
            }
        }
        return await this.model.adtaper.set(this.model.name, conditions, value, !!this.isOverride);
    }

    public async patch(value: Partial<T>): Promise<boolean> {
        if (this.limitCount !== undefined) {
            throw new Error("patch cannot set limit");
        }
        const { definition } = this.model;
        const conditions = this.condition.generate(this.model.definition);

        for (const key in value) {
            if (key in definition && !definition[key].isValid(value[key]!)) {
                throw new Error(`invalid value of ${JSON.stringify(key)}: ${value[key]}`);
            }
        }
        return await this.model.adtaper.update(this.model.name, conditions, value, !!this.isOverride);
    }

    public async delete(): Promise<boolean> {
        if (this.limitCount !== undefined) {
            throw new Error("delete cannot set limit");
        }
        if (this.isOverride !== undefined) {
            throw new Error("delete cannot set override");
        }
        const adapter = this.model.adtaper;
        const conditions = this.condition.generate(this.model.definition);

        if (adapter.delete) {
            return await adapter.delete(this.model.name, conditions);
        } else {
            const deletedCount = await adapter.deleteAll(this.model.name, conditions, 1);
            if (deletedCount > 0) {
                return true;
            } else {
                return false;
            }
        }
    }

    public async deleteAll(): Promise<number> {
        if (this.isOverride !== undefined) {
            throw new Error("deleteAll cannot set override");
        }
        const adapter = this.model.adtaper;
        const conditions = this.condition.generate(this.model.definition);
        return await adapter.deleteAll(this.model.name, conditions, this.limitCount);
    }

}
