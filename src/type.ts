
/*
 * The type of consume. PS: "pop" get the message and delete it from database, "read" get the message and keep it on database
 */
type CONSUME_TYPE = "pop" | "read";

interface Options {
    /**
     * The name of the queue
     */
    queueName: string;
    /**
     * The visibility time of the message. PS: is the time in seconds that the message will be invisible to other consumers
     */
    visibilityTime?: number;
    /**
     * The type of consume.
     */
    consumeType: CONSUME_TYPE;
    /**
     * The pool size. PS: the number of messages that will be consumed at the same time
     */
    poolSize?: number;
    /**
     * The time to wait before next polling. PS: the time in milliseconds
     */
    timeMsWaitBeforeNextPolling?: number;

    /**
     * The enabled polling. PS: if true, the consumer will poll the message
     */
    enabledPolling: boolean;

    /**
     * The name of the queue DLQ
     */
    queueNameDlq?: string;

    /**
     * The total retries before send the message to DLQ. PS: if set queueNameDlq, this option is required
     */
    totalRetriesBeforeSendToDlq?: number;
}

/**
 * The callback function to handle the message
 * @param message The message
 * @param signal The abort signal
 * @returns Promise<void>
 */
type HandlerCallback = (message: { [key: string]: any }, signal: AbortSignal) => Promise<void>

interface Message {
    /**
     * The message ID
     */
    msg_id: number;
    /**
     * The number of times the message has been read
     */
    read_ct: number;
    /**
     * The time the message was enqueued
     */
    enqueued_at: string;
    /**
     * The visibility time of the message
     */
    vt: string;
    /**
     * The message content
     */
    message: { [key: string]: any };
}

interface QueueDriver {

    /**
     * Send the message
     * @param queueName The name of the queue
     * @param message The message
     * @returns Promise<{ error: any }>
     */
    send(
        queueName: string,
        message: { [key: string]: any },
        signal: AbortSignal
    ): Promise<{ error: any }>;

    /**
     * Get the message
     * @param queueName The name of the queue
     * @param visibilityTime The visibility time of the message
     * @param totalMessages The total messages to get
     * @returns Promise<{ data: Message[], error: any }>
     */
    get(
        queueName: string,
        visibilityTime: number,
        totalMessages: number
    ): Promise<{ data: Message[], error: any }>;

    /**
     * Pop the message
     * @param queueName The name of the queue
     * @returns Promise<{ data: Message[], error: any }>
     */
    pop(
        queueName: string,
    ): Promise<{ data: Message[], error: any }>;

    /**
     * Delete the message
     * @param queueName The name of the queue
     * @param messageID The message ID
     * @returns Promise<{ error: any }>
     */
    delete(
        queueName: string,
        messageID: number
    ): Promise<{ error: any }>;
}

export type { Options, HandlerCallback, Message, QueueDriver }