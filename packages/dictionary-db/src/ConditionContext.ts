import type { ConditionBuilder, ConditionFiller, ModelDefinition } from "./Model";
import type { Condition, Conditions } from "./DatabaseAdapter";

export class ConditionContext<B, T extends { [key: string]: any }> {

    private readonly nodes: ConditionNode<T, keyof T>[] = [];
    private willIncludesAll: boolean = false;

    public constructor(
        public readonly builder: B,
    ) { }

    public includesAll(): void {
        if (this.nodes.length > 0) {
            throw new Error("cannot add more condition when includes all");
        }
        this.willIncludesAll = true;
    }

    public append(node: ConditionNode<T, keyof T>): void {
        if (this.willIncludesAll) {
            throw new Error("cannot add more condition when includes all");
        }
        this.nodes.push(node);
    }

    public createBuilder(isAnd?: boolean): ConditionBuilder<B, T> {
        return new ConditionBuilderImplement(this, isAnd);
    }

    public generate(definition: ModelDefinition<T>): Conditions<T> {
        const conditions: Condition<T, keyof T>[][] = [];
        let isFirst = true;

        for (const { isAnd, columeName, sign, value } of this.nodes) {
            if (isAnd === undefined) {
                if (!isFirst) {
                    throw new Error("cannot call duplicated .who()");
                }
                conditions.push([{ columeName, sign, value }]);
            } else {
                let latestConditions = conditions[conditions.length - 1];
                if (!latestConditions || (!isAnd && latestConditions.length > 0)) {
                    latestConditions = [];
                    conditions.push(latestConditions);
                }
                latestConditions.push({ columeName, sign, value });
            }
            isFirst = false;
        }
        for (const subConditions of conditions) {
            for (const { columeName, value } of subConditions) {
                const propertyDefinition = definition[columeName];
                if (!propertyDefinition) {
                    throw new Error(`invalid colume ${JSON.stringify(columeName)}`);
                }
                if (!propertyDefinition.isConditionable) {
                    throw new Error(`colume ${JSON.stringify(columeName)} cannot be condition`);
                }
                if (!propertyDefinition.isValid(value)) {
                    throw new Error(`invalid value of ${JSON.stringify(columeName)}: ${value}`);
                }
            }
        }
        return {
            includesAll: this.willIncludesAll,
            conditions,
        };
    }

}

type ConditionNode<T extends { [key: string]: any }, K extends keyof T> = Condition<T, K> & {
    readonly isAnd?: boolean;
};

class ConditionBuilderImplement<B, T extends { [key: string]: any }> implements ConditionBuilder<B, T> {

    public constructor(
        private readonly context: ConditionContext<B, T>,
        private readonly isAnd?: boolean,
    ) { }

    public all(): B {
        this.context.includesAll();
        return this.context.builder;
    }

    public colume<K extends keyof T>(name: K): ConditionFiller<B, T[K], T> {
        return new ConditionFillerImplement(this.context, name, this.isAnd);
    }

}

class ConditionFillerImplement<B, T extends { [key: string]: any }, K extends keyof T> implements ConditionFiller<B, T[K], T> {

    public constructor(
        private readonly context: ConditionContext<B, T>,
        private readonly columeName: K,
        private readonly isAnd?: boolean,
    ) { }

    public equals(value: T[K]): B {
        this.context.append({
            columeName: this.columeName,
            isAnd: this.isAnd,
            sign: "=",
            value,
        });
        return this.context.builder;
    }

    public notEqualTo(value: T[K]): B {
        this.context.append({
            columeName: this.columeName,
            isAnd: this.isAnd,
            sign: "!=",
            value,
        });
        return this.context.builder;
    }

    public greaterThan(value: T[K]): B {
        this.context.append({
            columeName: this.columeName,
            isAnd: this.isAnd,
            sign: ">",
            value,
        });
        return this.context.builder;
    }

    public greaterOrEqualsThan(value: T[K]): B {
        this.context.append({
            columeName: this.columeName,
            isAnd: this.isAnd,
            sign: ">=",
            value,
        });
        return this.context.builder;
    }

    public lessThan(value: T[K]): B {
        this.context.append({
            columeName: this.columeName,
            isAnd: this.isAnd,
            sign: "<",
            value,
        });
        return this.context.builder;
    }

    public lessOrEqualsThan(value: T[K]): B {
        this.context.append({
            columeName: this.columeName,
            isAnd: this.isAnd,
            sign: "<=",
            value,
        });
        return this.context.builder;
    }

}
