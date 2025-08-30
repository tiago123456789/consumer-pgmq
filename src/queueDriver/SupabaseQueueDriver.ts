import { Message, QueueDriver } from "../type";
import { SupabaseClient } from '@supabase/supabase-js';

class SupabaseQueueDriver implements QueueDriver {

    constructor(
        private supabase: SupabaseClient
    ) { }

    /**
     * Get the message
     * @param queueName The name of the queue
     * @param visibilityTime The visibility time of the message
     * @param totalMessages The total messages to get
     * @returns Promise<{ data: Message[], error: any }>
     */
    async get(queueName: string, visibilityTime: number, totalMessages: number): Promise<{ data: Message[]; error: any; }> {
        const { data, error } = await this.supabase.rpc("read", {
            queue_name: queueName,
            sleep_seconds: visibilityTime,
            n: totalMessages
        });

        return { data: data as Message[], error };
    }

    /**
     * Pop the message
     * @param queueName The name of the queue
     * @returns Promise<{ data: Message[], error: any }>
     */
    async pop(queueName: string): Promise<{ data: Message[]; error: any; }> {
        const { data, error } = await this.supabase.rpc("pop", {
            queue_name: queueName,
        });

        return { data: data as Message[], error };
    }

    /**
     * Delete the message
     * @param queueName The name of the queue
     * @param messageID The message ID
     * @returns Promise<{ error: any }>
     */
    async delete(queueName: string, messageID: number): Promise<{ error: any; }> {
        const { error } = await this.supabase.rpc("delete", {
            queue_name: queueName,
            message_id: messageID
        });

        return { error };
    }

}

export default SupabaseQueueDriver;
