import TableStore from "tablestore";

export class TableStoreLog {

    public constructor(
        private readonly printLog: (...args: string[]) => void,
    ) {}

    public log(methodName: string, params: any): void {
        try {
            const fillStrings: string[] = [
                methodName, params.tableName,
            ];
            if (params.condition) {
                this.parseCondition(fillStrings, params.condition);
            }
            if (params.primaryKey) {
                fillStrings.push("primary-keys:");
                this.parseSetterArray(fillStrings, params.primaryKey);
            }
            if (params.attributeColumns) {
                fillStrings.push("columes:");
                this.parseSetterArray(fillStrings, params.attributeColumns);
            }
            if (params.updateOfAttributeColumns) {
                this.parseUpdaterArray(fillStrings, params.updateOfAttributeColumns);
            }
            this.printLog.apply(null, fillStrings);

        } catch (error) {
            // 只是日志报不上去而已，不要因此阻止正常业务
            this.printLog("print error failed:", error.stack || error.message);
        }
    }

    private parseCondition(fillStrings: string[], condition: any): void {
        let str: string;

        switch (condition.rowExistenceExpectation) {
            case TableStore.RowExistenceExpectation.EXPECT_NOT_EXIST: {
                str = "expect not-exist";
                break;
            }
            case TableStore.RowExistenceExpectation.EXPECT_EXIST: {
                str = "expect exist";
                break;
            }
            case TableStore.RowExistenceExpectation.IGNORE: {
                str = "ignore";
                break;
            }
            default: {
                str = "expect " + condition.rowExistenceExpectation;
                break;
            }
        }
        fillStrings.push(`(${str})`);
    }

    private parseUpdaterArray(fillStrings: string[], updaterArray: any[]): void {
        for (const updaterObject of updaterArray) {
            for (const method in updaterObject) {
                fillStrings.push(method.toLowerCase() + ":");
                this.parseSetterArray(fillStrings, updaterObject[method]);
            }
        }
    }

    private parseSetterArray(fillStrings: string[], setterArray: any[]): void {
        for (const setter of setterArray) {
            if (typeof setter === "string") {
                fillStrings.push(setter);
            } else {
                for (const key in setter) {
                    fillStrings.push(key + "=" + this.parseTableStoreValue(setter[key]));
                }
            }
        }
    }

    private parseTableStoreValue(value: any): string {
        if (typeof value === "object" && value !== null) {
            return `${value}`;
        } else {
            return JSON.stringify(value);
        }
    }

}
