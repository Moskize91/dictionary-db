import type { DeleteItemCommandInput, PutItemCommandInput, ScanCommandInput, UpdateItemCommandInput } from "@aws-sdk/client-dynamodb";

export class DynamoStoreLog {

    public constructor(
        private readonly printLog: (...args: string[]) => void,
    ) {}

    public log(methodName: string, params: DeleteItemCommandInput | ScanCommandInput | UpdateItemCommandInput | PutItemCommandInput): void {
        try {
            const fillStrings: string[] = [
                methodName, params.TableName || "no table name",
            ];
            if ((params as any).Key) {
                fillStrings.push(JSON.stringify((params as any).Key));
            }

            if (params.ConditionalOperator) {
                fillStrings.push(params.ConditionalOperator);
            }

            if (params.ExpressionAttributeNames) {
                fillStrings.push(JSON.stringify(params.ExpressionAttributeNames));
            }

            if (params.ExpressionAttributeValues) {
                // TODO: 是否需要处理隐私信息？
                fillStrings.push(JSON.stringify(params.ExpressionAttributeValues));
            }

            this.printLog.apply(null, fillStrings);

        } catch (error) {
            // 只是日志报不上去而已，不要因此阻止正常业务
            this.printLog("print error failed:", error.stack || error.message);
        }
    }

}
