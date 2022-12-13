import { expect } from "chai";

export async function expectError(errorMessage: string, handler: () => Promise<void>): Promise<void> {
    try {
        await handler();

    } catch (error) {
        expect(errorMessage).equals(error.message);
        return;
    }
    throw new Error(`want throw Error "${errorMessage}", but get nothing`);
}
