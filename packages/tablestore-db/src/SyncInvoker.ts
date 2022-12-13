export type Invoker<R> = () => Promise<R>;

type Node<R> = {
    readonly invoker: Invoker<R>;
    readonly resolve: (result: R) => void;
    readonly reject: (error: Error) => void;
};

export class SyncInvoker {

    private readonly queue: Node<any>[] = [];

    private _seatsLimit: number = 0;
    private seatsCount: number = 0;

    public get paddingCount(): number {
        return this.queue.length;
    }

    public get seatsLimit(): number {
        return this._seatsLimit;
    }

    public set seatsLimit(seatsLimit: number) {
        if (this._seatsLimit !== seatsLimit) {
            this._seatsLimit = seatsLimit;
            this.fireUpdate();
        }
    }

    public cleanPadding(count: number): void {
        this.queue.splice(0, count);
    }

    public invoke<R>(invoker: Invoker<R>): Promise<R> {
        return this.invokeWith(invoker, false);
    }

    public invokePriority<R>(invoker: Invoker<R>): Promise<R> {
        return this.invokeWith(invoker, true);
    }

    private invokeWith<R>(invoker: Invoker<R>, isPriority: boolean): Promise<R> {
        return new Promise((resolve, reject) => {
            const node: Node<R> = { invoker, resolve, reject };
            if (isPriority) {
                this.queue.unshift(node);
            } else {
                this.queue.push(node);
            }
            this.fireUpdate();
        });
    }

    private fireUpdate(): void {
        if (this.seatsCount < this._seatsLimit) {
            const node = this.queue.shift()!;
            if (node) {
                this.seatsCount += 1;
                node.invoker()
                    .then((result) => {
                        node.resolve(result);
                        this.seatsCount -= 1;
                        this.fireUpdate();
                    })
                    .catch((error) => {
                        node.reject(error);
                        this.seatsCount -= 1;
                        this.fireUpdate();
                    });
            }
        }
    }

}
