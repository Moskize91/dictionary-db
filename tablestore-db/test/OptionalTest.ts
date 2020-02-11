import chaiAsPromised from "chai-as-promised";

import type { Suite } from "mocha";
import { expect, use } from "chai";
import { db } from "./Models";
import { expectError } from "./ExceptError";

use(chaiAsPromised);

// eslint-disable-next-line func-names
describe("optional test", function (this: Suite): void {
    this.timeout(10 * 60 * 1000);

    it("optional test", async () => {
        const memberDB = db.model("members");
        await memberDB.set.all().deleteAll();

        await expectError("invalid value of \"id\": undefined", async () => {
            await memberDB.post({
                name: "Tao Zeyu",
                city: "Xiangtan",
            } as any);
        });

        await expectError("invalid value of \"name\": undefined", async () => {
            await memberDB.post({
                id: "001",
                city: "Xiangtan",
            } as any);
        });

        // age 是可选的，我可以不填，也能创建
        await memberDB.post({
            id: "001",
            name: "Tao Zeyu",
            city: "Xiangtan",
        });
        expect(await memberDB.get.colume("id").equals("001").result()).deep.equals({
            age: undefined,
            id: "001",
            name: "Tao Zeyu",
            city: "Xiangtan",
        });

        // age 是可选的，填写了，也能创建
        await memberDB.post({
            id: "002",
            name: "Tao Zeyu",
            age: 100,
            city: "Xiangtan",
        });
        expect(await memberDB.get.colume("id").equals("002").result()).deep.equals({
            id: "002",
            name: "Tao Zeyu",
            age: 100,
            city: "Xiangtan",
        });

        // city 有默认值 Shanghai，即便不填也会自动填充
        await memberDB.post({
            id: "003",
            name: "Tao Zeyu",
            age: 100,
        } as any);
        expect(await memberDB.get.colume("id").equals("003").result()).deep.equals({
            id: "003",
            name: "Tao Zeyu",
            age: 100,
            city: "Shanghai",
        });
    });
});
