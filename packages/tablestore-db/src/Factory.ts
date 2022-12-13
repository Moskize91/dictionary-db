import type { DatabaseAdapter } from "dictionary-db";
import type { DynamoOptions } from "./dynamo";
import type { TableStoreModelDefinition } from "./TableStoreType";
import { TablestoreAdapterFactory } from "./tablestore";
import { DynamoAdapterFactory } from "./dynamo";
import type { TablestoreOptions } from "./tablestore";

function isDynamoOptions(options: TablestoreOptions | DynamoOptions): options is DynamoOptions {
    return !!(options as DynamoOptions).dynamodb;
}

export type DatabaseOptions = TablestoreOptions | DynamoOptions;

export class DatabaseAdapterFactory<MODELS extends { [key: string]: { [key: string]: any } }> {

    private readonly modelNodes: any;

    public constructor(modelTemplate: TableStoreModelDefinition<MODELS>) {
        this.modelNodes = Object.freeze(modelTemplate);
    }

    public create(options: TablestoreOptions | DynamoOptions): DatabaseAdapter<MODELS> {
        if (isDynamoOptions(options)) {
            return new DynamoAdapterFactory<MODELS>(this.modelNodes).create(options);
        } else {
            return new TablestoreAdapterFactory<MODELS>(this.modelNodes).create(options);
        }
    }

}
