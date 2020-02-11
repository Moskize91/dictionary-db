import TableStore from "tablestore";

import type { DatabaseAdapter, GetDescription, Conditions, ModelDefinition, PropertyDefinition } from "netless-dictionary-db";
import type { PrimaryKeyRange } from "./TableStoreParser";
import type { TableStoreModel } from "./TableStoreType";
import type { KeysDescription } from "./ModelNode";
import {
    parseColumes,
    parseModelObject,
    parseConditionsToRange,
    parseModelProperties,
    parseConditionsToPrimaryKey,
} from "./TableStoreParser";

import { isTablestoreValueEquals as isTableStoreValueEquals } from "./TableStoreType";
import { ModelNode } from "./ModelNode";
import { SyncInvoker } from "./SyncInvoker";
import { conditionsToString } from "./ConditionPrinter";
import { TableStoreLog } from "./TableStoreLog";

const DefaultSlicesCount = 1024;
const DeleteSlicesCount = 256;

const RetryCount = 26;
const RetryInterval = 500;
const RetryRandomRate = 1.2;
const SubOperationsSeatCount = 24;

type RangeOfDataResult = {
    readonly nextStartPrimaryKey?: any[];
    readonly rows: any[];
};

export type TableStoreModelDefinition<MODELS extends { [key: string]: any }> = {
    readonly [K in keyof MODELS]: TableStoreModel<MODELS[K]>;
};

export type TablestoreOptions = {
    readonly accessKeyId: string;
    readonly secretAccessKey: string;
    readonly instancename: string;
    readonly endpoint: string;
    readonly logPrinter?: (...args: string[]) => void;
};

export class TablestoreAdapterFactory<MODELS extends { [key: string]: any }> {

    private readonly modelNodes: { readonly [K in keyof MODELS]: ModelNode<MODELS[K]> };

    public constructor(modelTemplate: TableStoreModelDefinition<MODELS>) {
        const modelNodes: { [K in keyof MODELS]?: ModelNode<MODELS[K]> } = {};
        for (const modelName in modelTemplate) {
            modelNodes[modelName] = new ModelNode(modelName, modelTemplate[modelName]);
        }
        this.modelNodes = Object.freeze({ ...modelNodes }) as { readonly [K in keyof MODELS]: ModelNode<MODELS[K]> };
    }

    public create(options: TablestoreOptions): DatabaseAdapter<MODELS> {
        return new TablestoreAdapter(new TableStore.Client(options), this.modelNodes, options.logPrinter);
    }

}

class TablestoreAdapter<MODELS extends Object> implements DatabaseAdapter<MODELS> {

    private readonly client: any;
    private readonly modelNodes: { readonly [K in keyof MODELS]: ModelNode<MODELS[K]> };
    private readonly log?: TableStoreLog;
    private readonly sync: SyncInvoker = new SyncInvoker();

    public constructor(client: any, modelNodes: { readonly [K in keyof MODELS]: ModelNode<MODELS[K]> },
                       logPrinter?: (...args: string[]) => void) {
        this.client = client;
        this.modelNodes = modelNodes;
        this.log = logPrinter && new TableStoreLog(logPrinter);
        this.sync.seatsLimit = SubOperationsSeatCount;
    }

    public getModelDefinitions(): { readonly [K in keyof MODELS]: ModelDefinition<MODELS[K]> } {
        const definitions: { [K in keyof MODELS]?: ModelDefinition<MODELS[K]> } = {};

        for (const modelName in this.modelNodes) {
            const modelNode = this.modelNodes[modelName];
            const modelDefinitions: { [property: string]: PropertyDefinition<any> } = {};

            for (const key in modelNode.keys) {
                modelDefinitions[key] = Object.freeze({
                    isConditionable: true,
                    isValid: modelNode.keys[key]!.isValid,
                });
            }
            for (const colume in modelNode.columes) {
                modelDefinitions[colume] = Object.freeze({
                    isConditionable: modelNode.isIndexKey(colume),
                    isValid: modelNode.columes[colume]!.isValid,
                });
            }
            definitions[modelName] = modelDefinitions as any;
        }
        return definitions as { readonly [K in keyof MODELS]: ModelDefinition<MODELS[K]> };
    }

    public async get<M extends keyof MODELS>(modelName: M, description: GetDescription<MODELS[M]>): Promise<MODELS[M] | null> {
        const modelNode = this.modelNodes[modelName];
        const keysDescription = this.modelNodes[modelName].keysDescription(description.conditions);
        let primaryKeys: any[] | null = null;

        if (!keysDescription.isIndex) {
            primaryKeys = parseConditionsToPrimaryKey(keysDescription.combinedKeys, description.conditions);

        } else {
            const direction = this.direction(description.isAscending);
            const range = parseConditionsToRange(keysDescription.combinedKeys, description.conditions);
            const { rows } = await this.rangeOfData(keysDescription.name, direction, range, 1);

            if (rows.length > 0) {
                primaryKeys = this.pickIntoPrimaryKeys(modelNode, rows[0].primaryKey);
            }
        }
        let result: MODELS[M] | null = null;

        if (primaryKeys) {
            const row = await this.rowOfDate(modelName as string, primaryKeys);
            if (row) {
                result = parseModelProperties(modelNode, {
                    primaryKeys: row.primaryKey,
                    attributeColumns: row.attributes,
                });
            }
        }
        return result;
    }

    public async getAll<M extends keyof MODELS>(modelName: M,
                                                description: GetDescription<MODELS[M]>): Promise<ReadonlyArray<MODELS[M]>> {
        const direction = this.direction(description.isAscending);
        const modelNode = this.modelNodes[modelName];
        const keysDescription = modelNode.keysDescription(description.conditions);
        const range = parseConditionsToRange(keysDescription.combinedKeys, description.conditions);
        const { rows } = await this.rangeOfData(keysDescription.name, direction, range, description.limitCount);
        const results: MODELS[M][] = [];

        if (keysDescription.isIndex) {
            const promises: Promise<any | null>[] = [];

            for (const { primaryKey } of rows) {
                const primaryKeys = this.pickIntoPrimaryKeys(modelNode, primaryKey);
                const promise = this.sync.invoke(() => this.rowOfDate(modelName as string, primaryKeys));
                promises.push(promise);
            }
            for (const row of await Promise.all(promises)) {
                if (row) {
                    results.push(parseModelProperties(modelNode, {
                        primaryKeys: row.primaryKey,
                        attributeColumns: row.attributes,
                    }));
                }
            }
        } else {
            for (const row of rows) {
                results.push(parseModelProperties(modelNode, {
                    primaryKeys: row.primaryKey,
                    attributeColumns: row.attributes,
                }));
            }
        }
        return results;
    }

    public async getWithSlices<M extends keyof MODELS>(modelName: M, description: GetDescription<MODELS[M]>,
                                                       handler: (slices: readonly MODELS[M][],
                                                           stop: () => void) => Promise<void>): Promise<number> {

        const direction = this.direction(description.isAscending);
        const modelNode = this.modelNodes[modelName];
        const keysDescription = modelNode.keysDescription(description.conditions);
        const range = parseConditionsToRange(keysDescription.combinedKeys, description.conditions);
        const slicesCount = description.slicesCount !== undefined ? description.slicesCount : DefaultSlicesCount;
        const { limitCount } = description;

        let warppedHandler: (rows: any[], stop: () => void) => Promise<void>;

        if (keysDescription.isIndex) {
            warppedHandler = async (rows: any[], stop: () => void): Promise<void> => {
                const promises: Promise<any | null>[] = [];
                const slices: MODELS[M][] = [];

                for (const { primaryKey } of rows) {
                    const primaryKeys = this.pickIntoPrimaryKeys(modelNode, primaryKey);
                    const promise = this.sync.invoke(() => this.rowOfDate(modelName as string, primaryKeys));
                    promises.push(promise);
                }
                for (const row of await Promise.all(promises)) {
                    if (row) {
                        slices.push(parseModelProperties(modelNode, {
                            primaryKeys: row.primaryKey,
                            attributeColumns: row.attributes,
                        }));
                    }
                }
                await handler(slices, stop);
            };
        } else {
            warppedHandler = async (rows: any[], stop: () => void): Promise<void> => {
                const slices = rows.map((row) => parseModelProperties(modelNode, {
                    primaryKeys: row.primaryKey,
                    attributeColumns: row.attributes,
                }));
                await handler(slices, stop);
            };
        }
        return await this.iterateSlices(keysDescription.name as M, range, warppedHandler, direction, slicesCount, limitCount);
    }

    public async create<M extends keyof MODELS>(modelName: M, target: MODELS[M], isOverride: boolean): Promise<boolean> {
        const { primaryKeys, attributeColumns } = parseModelObject(this.modelNodes[modelName], target);
        const expectation = isOverride ? TableStore.RowExistenceExpectation.IGNORE :
            TableStore.RowExistenceExpectation.EXPECT_NOT_EXIST;
        const params = {
            tableName: modelName,
            condition: new TableStore.Condition(expectation, null),
            primaryKey: primaryKeys,
            attributeColumns,
            returnContent: { returnType: TableStore.ReturnType.Primarykey },
        };
        return await this.executeUpdate("putRow", params);
    }

    public async set<M extends keyof MODELS>(modelName: M, conditions: Conditions<MODELS[M]>,
                                             target: MODELS[M], isOverride: boolean): Promise<boolean> {
        const { columes } = this.modelNodes[modelName];
        for (const columeName in columes) {
            if (!(columeName in target)) {
                throw new Error(`expect colume name ${JSON.stringify(columeName)}`);
            }
        }
        return await this.update(modelName, conditions, target, isOverride);
    }

    public async update<M extends keyof MODELS>(modelName: M, conditions: Conditions<MODELS[M]>,
                                                target: Partial<MODELS[M]>, isOverride: boolean): Promise<boolean> {
        const modelNode = this.modelNodes[modelName];
        const keysDescription = modelNode.keysDescription(conditions);
        const updateOfAttributeColumns = parseColumes(keysDescription, modelNode.columes, conditions, target);
        const expectation = isOverride ? TableStore.RowExistenceExpectation.IGNORE :
            TableStore.RowExistenceExpectation.EXPECT_EXIST;
        if (updateOfAttributeColumns.length === 0) {
            return true; // 如果更新列表为空，不需要更新也可以视为成功

        } else {
            let primaryKeys: any[] | null = null;

            if (keysDescription.isIndex) {
                primaryKeys = await this.primaryKeysWithIndexesQuery(modelNode, conditions, keysDescription);
            } else {
                primaryKeys = parseConditionsToPrimaryKey(keysDescription.combinedKeys, conditions);
            }
            let success: boolean = false;

            if (primaryKeys) {
                const params: any = {
                    tableName: modelName,
                    condition: new TableStore.Condition(expectation, null),
                    primaryKey: primaryKeys,
                    updateOfAttributeColumns,
                };
                success = await this.executeUpdate("updateRow", params);
            }
            return success;
        }
    }

    public async delete<M extends keyof MODELS>(modelName: M, conditions: Conditions<MODELS[M]>): Promise<boolean> {
        const modelNode = this.modelNodes[modelName];
        const keysDescription = modelNode.keysDescription(conditions);
        let primaryKeys: any[] | null = null;

        if (keysDescription.isIndex) {
            primaryKeys = await this.primaryKeysWithIndexesQuery(modelNode, conditions, keysDescription);
        } else {
            primaryKeys = parseConditionsToPrimaryKey(keysDescription.combinedKeys, conditions);
        }
        const params: any = {
            tableName: modelName,
            condition: new TableStore.Condition(TableStore.RowExistenceExpectation.EXPECT_EXIST, null),
            primaryKey: primaryKeys,
        };
        return await this.executeUpdate("deleteRow", params);
    }

    private async primaryKeysWithIndexesQuery(modelNode: ModelNode<any>, conditions: Conditions<any>,
                                              keysDescription: KeysDescription<any>): Promise<any[] | null> {
        let primaryKeys: any[] | null = null;

        const direction = this.direction(true);
        const range = parseConditionsToRange(keysDescription.combinedKeys, conditions);
        const { rows } = await this.rangeOfData(keysDescription.name, direction, range, 2);

        if (rows.length >= 2) {
            throw new Error(`not unique conditions: ${conditionsToString(conditions.conditions)}`);

        } else if (rows.length > 0) {
            primaryKeys = this.pickIntoPrimaryKeys(modelNode, rows[0].primaryKey);
        }
        return primaryKeys;
    }

    public async deleteAll<M extends keyof MODELS>(modelName: M, conditions: Conditions<MODELS[M]>,
                                                   limitCount?: number): Promise<number> {
        let deleteCount = 0;
        const direction = TableStore.Direction.FORWARD;
        const modelNode = this.modelNodes[modelName];
        const keysDescription = modelNode.keysDescription(conditions);
        const range = parseConditionsToRange(keysDescription.combinedKeys, conditions);
        const condition = new TableStore.Condition(TableStore.RowExistenceExpectation.EXPECT_EXIST, null);

        const handler = async (rows: any[]): Promise<void> => {
            const promises: Promise<boolean>[] = [];

            for (const row of rows) {
                let primaryKeys: any[];
                if (keysDescription.isIndex) {
                    primaryKeys = this.pickIntoPrimaryKeys(modelNode, row.primaryKey);
                } else {
                    primaryKeys = [];
                    for (const { name, value } of row.primaryKey) {
                        primaryKeys.push({ [name]: value });
                    }
                }
                promises.push(this.sync.invoke(() => this.executeUpdate("deleteRow", {
                    tableName: modelName,
                    condition,
                    primaryKey: primaryKeys,
                })));
            }
            for (const success of await Promise.all(promises)) {
                if (success) {
                    deleteCount += 1;
                }
            }
        };
        await this.iterateSlices(keysDescription.name as M, range, handler, direction, DeleteSlicesCount, limitCount);

        return deleteCount;
    }

    private direction(isAscending: boolean = true): any {
        if (isAscending) {
            return TableStore.Direction.FORWARD;
        } else {
            return TableStore.Direction.BACKWARD;
        }
    }

    private async iterateSlices<M extends keyof MODELS>(modelName: M, range: PrimaryKeyRange,
                                                        handler: (rows: any[], stop: () => void) => Promise<void>,
                                                        direction: any, slicesCount: number,
                                                        limitCount?: number): Promise<number> {
        let gotCount = 0;
        let shouldStop = false;

        function stop(): void {
            shouldStop = true;
        }
        while (true) {
            let slicesLimitCount: number;

            if (limitCount !== undefined) {
                slicesLimitCount = limitCount - gotCount;
                if (slicesLimitCount > slicesCount) {
                    slicesLimitCount = slicesCount;
                }
            } else {
                slicesLimitCount = slicesCount;
            }
            const { rows, nextStartPrimaryKey } = await this.rangeOfData(modelName as string, direction, range, slicesLimitCount);

            if (rows.length <= 0) {
                break;
            }
            gotCount += rows.length;

            await handler(rows, stop);

            if (limitCount !== undefined && gotCount >= limitCount) {
                break;
            }
            if (shouldStop || !nextStartPrimaryKey) {
                break;
            }
            for (let i = 0; i < range.startPrimaryKey.length; ++ i) {
                const cell = range.startPrimaryKey[i];
                const nextCell = nextStartPrimaryKey[i];

                for (const key in cell) {
                    cell[key] = nextCell.value;
                }
            }
        }
        return gotCount;
    }

    private rowOfDate(tableName: string, primaryKeys: any[]): Promise<any | null> {
        const params: any = {
            tableName,
            primaryKey: primaryKeys,
        };
        return new Promise((resolve, reject) => this.client.getRow(params, (error: Error | null, data: any) => {
            if (error) {
                reject(error);
            } else {
                const { row } = data;
                if (row.primaryKey && row.attributes) {
                    resolve(row);
                } else {
                    resolve(null);
                }
            }
        }));
    }

    private rangeOfData(tableName: string, direction: any, range: PrimaryKeyRange, limit?: number): Promise<RangeOfDataResult> {
        return new Promise((resolve, reject) => {
            if (this.isRangeEquals(range)) {
                const params: any = {
                    tableName,
                    primaryKey: range.startPrimaryKey,
                };
                this.client.getRow(params, (error: Error | null, data: any) => {
                    if (error) {
                        reject(error);
                    } else {
                        const { row } = data;
                        const rows: any[] = [];

                        if (row.primaryKey && row.attributes) {
                            rows.push(row);
                        }
                        resolve({ rows });
                    }
                });
            } else {
                const params: any = {
                    tableName,
                    direction,
                };
                switch (direction) {
                    case TableStore.Direction.FORWARD: {
                        params.inclusiveStartPrimaryKey = range.startPrimaryKey;
                        params.exclusiveEndPrimaryKey = range.endPrimaryKey;
                        break;
                    }
                    case TableStore.Direction.BACKWARD: {
                        params.inclusiveStartPrimaryKey = range.endPrimaryKey;
                        params.exclusiveEndPrimaryKey = range.startPrimaryKey;
                        break;
                    }
                }
                if (limit !== undefined) {
                    params.limit = limit;
                }
                this.client.getRange(params, (error: Error | null, data: any) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve({
                            nextStartPrimaryKey: data.nextStartPrimaryKey,
                            rows: data.rows,
                        });
                    }
                });
            }
        });
    }

    private pickIntoPrimaryKeys(modelNode: ModelNode<any>, primaryKey: any[]): any[] {
        const primaryKeys: any[] = [];
        for (const { name, value } of primaryKey as any[]) {
            if (modelNode.keys[name]) {
                primaryKeys.push({ [name]: value });
            }
        }
        return primaryKeys;
    }

    private isRangeEquals({ startPrimaryKey, endPrimaryKey }: PrimaryKeyRange): boolean {
        if (startPrimaryKey.length !== endPrimaryKey.length) {
            return false;

        } else {
            for (let i = 0; i < startPrimaryKey.length; ++ i) {
                const startKey = startPrimaryKey[i];
                const endKey = endPrimaryKey[i];

                for (const key in startKey) {
                    if (!isTableStoreValueEquals(startKey[key], endKey[key])) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private async executeUpdate(methodName: string, params: any): Promise<boolean> {
        this.log?.log(methodName, params);
        let conflictError: Error | undefined;

        for (let i = 0; i < RetryCount; ++ i) {
            const resolveResult = await new Promise<boolean | undefined>((resolve, reject) => {
                this.client[methodName](params, (error: Error | undefined) => {
                    if (error) {
                        if (/OTSConditionCheckFail/i.test(error.message)) {
                            resolve(false);
                        } else if (/OTSRowOperationConflict/i.test(error.message)) {
                            conflictError = error;
                            resolve(undefined);
                        } else {
                            reject(error);
                        }
                    } else {
                        resolve(true);
                    }
                });
            });
            if (resolveResult !== undefined) {
                return resolveResult;
            }
            if (i < RetryCount - 1) {
                const sleepInterval = RetryInterval * (1.0 + RetryRandomRate * Math.random());
                await new Promise((resolve) => setTimeout(resolve, sleepInterval));
            }
        }
        throw new Error(`${conflictError!.message} (after ${RetryCount} times retries)`);
    }

}
