import { Knex } from "knex";
import { Message, QueueDriver } from "../type";

class PostgresQueueDriver implements QueueDriver {


    constructor(
        private connection: Knex,
        private schema: string = "public"
    ) { }

    /**
     * Get the message
     * @param queueName The name of the queue
     * @param visibilityTime The visibility time of the message
     * @param totalMessages The total messages to get
     * @returns Promise<{ data: Message[], error: any }>
     */
    async get(queueName: string, visibilityTime: number, totalMessages: number): Promise<{ data: Message[]; error: any; }> {
        try {
            const register = await this.connection.raw(`
                SELECT * FROM ${this.schema}.read(
                    queue_name => ?,
                    vt         => ?,
                    qty        => ?
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
            const register = await this.connection.raw(`
                SELECT * FROM ${this.schema}.pop(
                    queue_name => ?
                );
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
            await this.connection.raw(`
            SELECT * FROM ${this.schema}.delete(
                queue_name => ?,
                msg_id     => ?
            );
            `, [queueName, messageID]
            )

            return { error: null };
        } catch (error) {
            return { error };
        }
    }

}

export default PostgresQueueDriver;
