
import { EventEmitter } from 'events';
import { HandlerCallback, Message, Options, QueueDriver } from './type';

/**
 * The consumer class.
 */
class Consumer extends EventEmitter {

    /**
     * The callback function to handle the message.
     */
    private callback: HandlerCallback
    /**
     * The options to configure the consumer.
     */
    private options: Options
    /**
     * The supabase client.
     */
    private client: QueueDriver

    constructor(
        options: Options,
        callback: HandlerCallback,
        client: QueueDriver,
    ) {
        super();
        this.options = options;
        this.callback = callback;
        this.client = client;
    }

    /**
     * Get the message
     * @returns Promise<{ data: Message[], error: any }>
     * @private
     */
    private async getMessage(): Promise<{ data: Message[], error: any }> {
        if (this.options.consumeType === "read") {
            if (!this.options.visibilityTime) {
                throw new Error("visibilityTime is required for read");
            }

            const { data, error } = await this.client.get(
                this.options.queueName, this.options.visibilityTime,
                this.options.poolSize || 1
            );

            return { data: data as Message[], error };
        }

        const { data, error } = await this.client.pop(this.options.queueName);
        return { data: data as Message[], error };
    }

    /**
     * @param data 
     * @returns Promise<void>
     * @private
     */
    private async deleteMessage(data: Message, signal: AbortSignal) {
        if (signal.aborted) {
            return;
        }

        if (this.options.consumeType === "read") {
            const deleteMessage = await this.client.delete(
                this.options.queueName,
                data.msg_id
            );
            if (deleteMessage.error) {
                this.emit('error', deleteMessage.error);
                return;
            }
        }
    }

    /**
     * Poll the message
     * @returns Promise<void>
     * @private
     */
    private async pollMessage() {
        let promises: Promise<any>[] = [];

        try {
            const { data, error } = await this.getMessage();
            if (error) {
                throw error;
            }

            if (data.length === 0) {
                setTimeout(
                    () => this.pollMessage(),
                    (this.options.timeMsWaitBeforeNextPolling || 1000) * 10
                );
                return;
            }


            const controller = new AbortController();
            const signal = controller.signal;

            for (let i = 0; i < (data.length || 1); i++) {
                promises.push(
                    this.callback(data[i].message, signal).then(async () => {
                        await this.deleteMessage(data[i], signal);
                        this.emit('finish', data[i]);
                    })
                );
            }

            setTimeout(() => controller.abort(), (this.options.visibilityTime || 1) * 1000);
            await Promise.all(promises);
            promises = [];
        } catch (err: any) {
            if (err.name === "AbortError") {
                this.emit("abort-error", err)
            } else {
                this.emit('error', err);
            }
        } finally {
            setTimeout(() => this.pollMessage(), this.options.timeMsWaitBeforeNextPolling || 1000);
        }
    }

    /**
     * Start the consumer
     * @returns Promise<void>
     * @public
     */
    async start() {
        await this.pollMessage();
    }
}


export default Consumer;
