import type { Condition } from "netless-dictionary-db";

export function conditionsToString<T>(orConditions: ReadonlyArray<ReadonlyArray<Condition<T, keyof T>>>): string {
    const orStrings: string[] = [];
    for (const andConditions of orConditions) {
        const andStrings: string[] = [];
        for (const c of andConditions) {
            andStrings.push(`${c.columeName} ${c.sign} ${c.value}`);
        }
        orStrings.push(andStrings.join(" && "));
    }
    return orStrings.join(" || ");
}
