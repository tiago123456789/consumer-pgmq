import { Message, QueueDriver } from "../type";
import { Client } from "pg";

class PostgresQueueDriver implements QueueDriver {


    constructor(
        private connection: Client,
        private schema: string = "public",
    ) { }

    /**
     * Send the message
     * @param queueName The name of the queue
     * @param message The message
     * @returns Promise<{ error: any }>
     */
    async send(
        queueName: string,
        message: { [key: string]: any; },
    ): Promise<{ error: any; }> {
        try {
            await this.connection.query(`
            SELECT * FROM ${this.schema}.send(
                queue_name => $1,
                msg        => $2,
                delay      => $3
            );
            `, [queueName, message, 1]
            )

            return { error: null };
        } catch (error) {
            return { error };
        }
    }

    /**
     * Get the message
     * @param queueName The name of the queue
     * @param visibilityTime The visibility time of the message
     * @param totalMessages The total messages to get
     * @returns Promise<{ data: Message[], error: any }>
     */
    async get(queueName: string, visibilityTime: number, totalMessages: number): Promise<{ data: Message[]; error: any; }> {
        try {
            const register = await this.connection.query(`
                SELECT * FROM ${this.schema}.read(
                    queue_name => $1,
                    vt         => $2,
                    qty        => $3
                );
                `, [queueName, visibilityTime, totalMessages]
            )

            if (!register.rows) {
                return { data: [], error: null };
            }

            return { data: register.rows as Message[], error: null };
        } catch (error) {
            return { data: [], error };
        }

    }

    /**
     * Pop the message
     * @param queueName The name of the queue
     * @returns Promise<{ data: Message[], error: any }>
     */
    async pop(queueName: string): Promise<{ data: Message[]; error: any; }> {
        try {
            const register = await this.connection.query(`
                SELECT * FROM ${this.schema}.pop(
                    queue_name => $1
                )
                `, [queueName]
            )

            if (!register.rows) {
                return { data: [], error: null };
            }

            return { data: register.rows as Message[], error: null };
        } catch (error) {
            return { data: [], error };
        }

    }

    /**
     * Delete the message
     * @param queueName The name of the queue
     * @param messageID The message ID
     * @returns Promise<{ error: any }>
     */
    async delete(queueName: string, messageID: number): Promise<{ error: any; }> {
        try {
            await this.connection.query(`
            SELECT * FROM ${this.schema}.delete(
                queue_name => $1,
                msg_id     => $2
            );
            `, [queueName, messageID],

            )

            return { error: null };
        } catch (error) {
            return { error };
        }
    }

}

export default PostgresQueueDriver;
