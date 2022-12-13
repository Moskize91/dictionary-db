import type { DatabaseAdapter, GetDescription, Conditions, ModelDefinition, PropertyDefinition } from "dictionary-db";
import type { AttributeValue,
              DeleteItemCommandInput,
              DynamoDBClient,
              DynamoDBClientConfig,
              GetItemCommandInput,
              GetItemCommandOutput,
              PutItemCommandInput,
              QueryCommandInput,
              QueryCommandOutput,
              ScanCommandInput,
} from "@aws-sdk/client-dynamodb";
import { UpdateItemCommand,
         PutItemCommand,
         GetItemCommand,
         QueryCommand,
         DynamoDB,
         DeleteItemCommand,
         ScanCommand,
} from "@aws-sdk/client-dynamodb";

import { DynamoTable, isGsiIndex, isTableIndex } from "./DynamoTable";
import { addDeleteCommandCondition, parseColumns, parseConditionsToGetCommand,
         parseConditionsToRangeCommand,
         parseTargetToPutCommand,
         parseUpdateCommand } from "./DynamoParser";
import type { TablestoreOptions } from "../tablestore/TablestoreAdapter";
import { hasItemOutput,
         hasItemsOutput,
         isQueryCommandInput,
         transformToModelArray,
         transformToModelObject,
} from "./DynamoTransfer";
import { conditionsToString } from "../ConditionPrinter";
import type { TableStoreModelDefinition } from "../TableStoreType";
import { DynamoStoreLog } from "./DynamoLog";

const DefaultSlicesCount = 1024;
const DeleteSlicesCount = 256;

export type DynamoOptions = {
    logPrinter?: TablestoreOptions["logPrinter"];
    dynamodb: DynamoDBClientConfig;
};

const ConditionalCheckFailedException = "ConditionalCheckFailedException";

export class DynamoAdapterFactory<MODELS extends { [key: string]: { [key: string]: any } }> {

    private readonly tableNodes: { readonly [K in keyof MODELS]: DynamoTable<MODELS[K]> };

    public constructor(modelTemplate: TableStoreModelDefinition<MODELS>) {
        const tableNodes: { [K in keyof MODELS]?: DynamoTable<MODELS[K]> } = {};
        for (const tableName in modelTemplate) {
            tableNodes[tableName] = new DynamoTable(tableName, modelTemplate[tableName]);
        }
        this.tableNodes = Object.freeze({ ...tableNodes }) as { readonly [K in keyof MODELS]: DynamoTable<MODELS[K]> };
    }

    public create(options: DynamoOptions): DatabaseAdapter<MODELS> {
        return new DynamoAdapter(new DynamoDB(options.dynamodb), this.tableNodes, options.logPrinter);
    }

}

export class DynamoAdapter<MODELS extends { [key: string]: { [key: string]: any } }> implements DatabaseAdapter<MODELS> {

    private readonly client: DynamoDBClient;
    private readonly tableNodes: { readonly [K in keyof MODELS]: DynamoTable<MODELS[K]> };
    private readonly log?: DynamoStoreLog;
    // tablestore 本身的二级索引，不包含二级索引主键和 primaryKey 外的内容，所以 tablestore 代码中，需要拿到 primaryKey 的值，再去查询获取全体。
    // dynamo 在创建 gsi 的时候，可以选择是否包含所有值。只要选择了 all，就不需要二次查询。
    // syncInvoker 是用于降低第二次查询的频率的的频率，dynamo 不需要
    // private readonly sync: SyncInvoker = new SyncInvoker();

    public constructor(client: any, tableNodes: { readonly [K in keyof MODELS]: DynamoTable<MODELS[K]> },
                       logPrinter?: (...args: string[]) => void) {
        this.client = client;
        this.tableNodes = tableNodes;
        this.log = logPrinter ? new DynamoStoreLog(logPrinter) : undefined;
    }

    public getModelDefinitions(): { readonly [K in keyof MODELS]: ModelDefinition<MODELS[K]> } {
        const definitions: { [K in keyof MODELS]?: ModelDefinition<MODELS[K]> } = {};

        for (const tableName in this.tableNodes) {
            const table = this.tableNodes[tableName];
            const modelDefinitions: { [property: string]: PropertyDefinition<any> } = {};

            for (const key in table.keys) {
                modelDefinitions[key] = Object.freeze({
                    isConditionable: true,
                    isValid: table.keys[key]!.isValid,
                });
            }
            for (const column in table.columes) {
                modelDefinitions[column] = Object.freeze({
                    isConditionable: table.isIndexKey(column),
                    isValid: table.columes[column]!.isValid,
                });
            }
            definitions[tableName] = modelDefinitions as any;
        }
        return definitions as { readonly [K in keyof MODELS]: ModelDefinition<MODELS[K]> };
    }

    public async get<M extends keyof MODELS>(tableName: M, description: GetDescription<MODELS[M]>): Promise<MODELS[M] | null> {
        const keysDescription = this.tableNodes[tableName].keysDescription(description.conditions);
        const table = this.tableNodes[tableName];
        if (isGsiIndex(keysDescription)) {
            const queryCommand = parseConditionsToRangeCommand(keysDescription, description.conditions, !isTableIndex(keysDescription));
            queryCommand.Limit = 1;
            const item = await this.getItems(new QueryCommand(queryCommand));
            if (item && item.Items.length > 0) {
                return transformToModelObject(table, { Item: item.Items[0] });
            } else {
                return null;
            }
        } else {
            const getCommand = parseConditionsToGetCommand(keysDescription, description.conditions);
            const item = await this.getItem(new GetItemCommand(getCommand));
            return transformToModelObject(table, item);
        }
    }

    private scanForward(isAscending: boolean = true): boolean {
        return isAscending;
    }

    public async getAll<M extends keyof MODELS>(tableName: M,
                                                description: GetDescription<MODELS[M]>): Promise<ReadonlyArray<MODELS[M]>> {
        const { isAscending, limitCount } = description;
        const tableNode = this.tableNodes[tableName];
        const keysDescription = tableNode.keysDescription(description.conditions);

        const range: ScanCommandInput | QueryCommandInput = parseConditionsToRangeCommand(keysDescription, description.conditions);

        if (isQueryCommandInput(range)) {
            range.ScanIndexForward = this.scanForward(isAscending);
            range.Limit = limitCount;
            const command = new QueryCommand(range);
            const result = await this.getItems(command);
            return transformToModelArray(tableNode, result);
        } else if (this.scanForward(isAscending)) {
            // scan has no ascending option https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html
            // and after test scan seems to be descending by default, we need reverse the result, and slice limit count
            const command = new ScanCommand(range);
            const result = await this.getItems(command);
            const items = transformToModelArray(tableNode, result).reverse();
            return items.slice(0, limitCount);
        } else {
            range.Limit = limitCount;
            const command = new ScanCommand(range);
            const result = await this.getItems(command);
            return transformToModelArray(tableNode, result);
        }
    }

    public async getWithSlices<M extends keyof MODELS>(tableName: M, description: GetDescription<MODELS[M]>,
                                                       handler: (slices: readonly MODELS[M][],
                                                           stop: () => void) => Promise<void>): Promise<number> {

        const { isAscending } = description;
        const tableNode = this.tableNodes[tableName];
        const keysDescription = tableNode.keysDescription(description.conditions);
        const range: ScanCommandInput | QueryCommandInput = parseConditionsToRangeCommand(keysDescription, description.conditions);
        const slicesCount = description.slicesCount !== undefined ? description.slicesCount : DefaultSlicesCount;
        const { limitCount } = description;
        // scan has no ascending option https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html
        // and scan seems to be descending by default, we need reverse the result if is ascending, and slice limit count
        const waitFinishToReverse = !isQueryCommandInput(range) && !this.scanForward(isAscending);
        const resultItems: MODELS[M][] = [];

        const warpHandler = async (items: QueryCommandOutput["Items"], stop: () => void): Promise<void> => {
            if (items) {
                const slices = items.map((row) => {
                    return transformToModelObject(tableNode, { "Item": row });
                }) as MODELS[M][];
                if (waitFinishToReverse) {
                    resultItems.push(...slices);
                } else {
                    await handler(slices, stop);
                }
            } else if (!waitFinishToReverse) {
                await handler([], stop);
            }
        };

        const gotCount = await this.iterateSlices(range, warpHandler, slicesCount, limitCount);
        if (waitFinishToReverse) {
            let shouldStop = false;
            // eslint-disable-next-line no-inner-declarations
            function stop(): void {
                shouldStop = true;
            }
            const slicesItem = resultItems.reverse();
            while (slicesItem.length > 0 && !shouldStop) {
                await handler(slicesItem.splice(0, slicesCount), stop);
            }
            return gotCount;
        } else {
            return gotCount;
        }
    }

    public async create<M extends keyof MODELS>(tableName: M, target: MODELS[M], isOverride: boolean): Promise<boolean> {
        const tableNode = this.tableNodes[tableName];
        const putCommand: PutItemCommandInput = parseTargetToPutCommand(target, tableNode, isOverride);
        return await this.sendCommand(new PutItemCommand(putCommand));
    }

    public async set<M extends keyof MODELS>(tableName: M, conditions: Conditions<MODELS[M]>,
                                             target: MODELS[M], isOverride: boolean): Promise<boolean> {
        // 只能修改非主键内容，这里在业务上，强制要求所有 column 都需要被修改，其实不是非常合适，不过照旧逻辑先
        const { columes } = this.tableNodes[tableName];
        for (const columnName in columes) {
            if (!(columnName in target)) {
                throw new Error(`expect column name ${JSON.stringify(columnName)}`);
            }
        }
        return await this.update(tableName, conditions, target, isOverride);
    }

    public async update<M extends keyof MODELS>(tableName: M, conditions: Conditions<MODELS[M]>,
                                                target: Partial<MODELS[M]>, isOverride: boolean): Promise<boolean> {
        const tableNode = this.tableNodes[tableName];
        const keysDescription = tableNode.keysDescription(conditions);
        const actionColumns = parseColumns(tableNode.patchableKeys, tableNode.columes, conditions, target);
        // eslint-disable-next-line max-len
        if (Object.keys(actionColumns).length === 0) {
            return true; // 如果更新列表为空，不需要更新也可以视为成功
        } else {
            let baseInput: GetItemCommandInput;

            if (isGsiIndex(keysDescription)) {
                const getCommand = parseConditionsToRangeCommand(keysDescription, conditions, !isTableIndex(keysDescription));
                const result = await this.getItems(new QueryCommand(getCommand));

                if (result && result.Items.length >= 2) {
                    throw new Error(`not unique conditions: ${conditionsToString(conditions.conditions)}`);
                }
                // TODO: 要看下原始版本，应该给报错还是直接返回 false
                if (result && result.Items.length === 0) {
                    throw new Error(`not found conditions: ${conditionsToString(conditions.conditions)}`);
                }
                const item = result && result.Items[0];

                baseInput = {
                    TableName: tableName as string,
                    Key: {
                        [tableNode.hashKey]: item![tableNode.hashKey],
                    },
                };

                if (tableNode.rangeKey) {
                    baseInput.Key![tableNode.rangeKey] = item![tableNode.rangeKey];
                }
            } else {
                baseInput = parseConditionsToGetCommand(keysDescription, conditions);
            }

            const updateInput = parseUpdateCommand(baseInput, actionColumns, isOverride, tableNode);
            return await this.sendCommand(new UpdateItemCommand(updateInput));
        }
    }

    public async delete<M extends keyof MODELS>(tableName: M, conditions: Conditions<MODELS[M]>): Promise<boolean> {
        const tableNode = this.tableNodes[tableName];
        const keysDescription = tableNode.keysDescription(conditions);
        let deleteCommandInput: DeleteItemCommandInput = {} as any;
        if (isGsiIndex(keysDescription)) {
            const getCommand = parseConditionsToRangeCommand(keysDescription, conditions, !isTableIndex(keysDescription));
            getCommand.TableName = tableName as string;
            const result = await this.getItems(new QueryCommand(getCommand));
            if (!result || result.Items.length === 0) {
                return true;
            }
            if (result && result.Items.length >= 2) {
                throw new Error(`not unique conditions: ${conditionsToString(conditions.conditions)}`);
            }
            const item = result && result.Items[0];
            deleteCommandInput = {
                TableName: tableName as string,
                Key: {
                    [tableNode.hashKey]: item[tableNode.hashKey],
                },
            };
            if (tableNode.rangeKey) {
                deleteCommandInput.Key![tableNode.rangeKey] = item[tableNode.rangeKey];
            }
        } else {
            deleteCommandInput = parseConditionsToGetCommand(keysDescription, conditions);
            deleteCommandInput.TableName = tableName as string;
        }

        deleteCommandInput = addDeleteCommandCondition(deleteCommandInput, tableNode);

        return await this.sendCommand(new DeleteItemCommand(deleteCommandInput));
    }

    public async deleteAll<M extends keyof MODELS>(tableName: M, conditions: Conditions<MODELS[M]>,
                                                   limitCount?: number): Promise<number> {
        let deleteCount = 0;
        const tableNode = this.tableNodes[tableName];
        const keysDescription = tableNode.keysDescription(conditions);
        const range: QueryCommandInput = parseConditionsToRangeCommand(keysDescription, conditions);
        range.TableName = tableName as string;

        const handler = async (rows: Record<string, AttributeValue>[]): Promise<void> => {
            const promises: Promise<boolean>[] = [];

            for (const row of rows) {
                const DeleteItemCommandInput: DeleteItemCommandInput = {
                    TableName: tableNode.name,
                    Key: {
                        [tableNode.hashKey]: row[tableNode.hashKey],
                    },
                };
                if (tableNode.rangeKey) {
                    DeleteItemCommandInput.Key![tableNode.rangeKey] = row[tableNode.rangeKey];
                }
                promises.push(new Promise((resolve, reject) => {
                    const deleteCommand = new DeleteItemCommand(DeleteItemCommandInput);
                    this.sendCommand(deleteCommand).then((result) => {
                        resolve(result);
                    }).catch((err) => {
                        reject(err);
                    });
                }));
            }
            for (const success of await Promise.all(promises)) {
                if (success) {
                    deleteCount += 1;
                }
            }
        };
        await this.iterateSlices(range, handler, DeleteSlicesCount, limitCount);

        return deleteCount;
    }

    private async iterateSlices(range: QueryCommandInput | ScanCommandInput,
                                handler: (rows: Record<string, AttributeValue>[], stop: () => void) => Promise<void>,
                                slicesCount: number, limitCount?: number): Promise<number> {
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
            range.Limit = slicesLimitCount;
            const command = isQueryCommandInput(range) ? new QueryCommand(range) : new ScanCommand(range);
            const result = await this.getItems(command);

            if (!result || result.Items.length <= 0) {
                break;
            }
            gotCount += result.Items.length;

            await handler(result.Items, stop);

            if (limitCount !== undefined && gotCount >= limitCount) {
                break;
            }
            if (shouldStop || !result.LastEvaluatedKey) {
                break;
            }
            range.ExclusiveStartKey = result.LastEvaluatedKey;
        }
        return gotCount;
    }

    // 相当于 executeUpdate
    private sendCommand<T extends DeleteItemCommand | ScanCommand | UpdateItemCommand | PutItemCommand,
    >(command: T): Promise<boolean> {
        this.log?.log(command.constructor.name, command.input);
        return new Promise<boolean>((resolve, reject) => {
            // ts 类型推导，要求 command 满足 三个类型所有的结构，而不是任一结构
            this.client.send(command as any).then((result) => {
                // eslint-disable-next-line max-len
                if (result.$metadata.httpStatusCode && result.$metadata.httpStatusCode >= 200 && result.$metadata.httpStatusCode < 400) {
                    resolve(true);
                } else {
                    resolve(false);
                }
            }).catch((err) => {
                if (err.name === ConditionalCheckFailedException) {
                    resolve(false);
                } else {
                    reject(err);
                }
            });
        });
    }

    // eslint-disable-next-line max-len
    private getItem(command: GetItemCommand): Promise<null | GetItemCommandOutput & { Item: NonNullable<GetItemCommandOutput["Item"]> }> {
        return new Promise((resolve, reject) => {
            this.client.send(command, (err: any, data: GetItemCommandOutput) => {
                if (err) {
                    reject(err);
                } else if (hasItemOutput(data)) {
                    resolve(data);
                } else {
                    resolve(null);
                }
            });
        });
    }

    // eslint-disable-next-line max-len
    private getItems(command: QueryCommand | ScanCommand): Promise<null | QueryCommandOutput & { Items: NonNullable<QueryCommandOutput["Items"]> }> {
        return new Promise((resolve, reject) => {
            this.client.send(command, (err: any, data: QueryCommandOutput) => {
                if (err) {
                    reject(err);
                } else if (hasItemsOutput(data)) {
                    resolve(data);
                } else {
                    resolve(null);
                }
            });
        });
    }

}
