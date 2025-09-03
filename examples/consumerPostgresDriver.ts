import { config } from "dotenv"
config()

import Consumer from '../src/consumer';
import PostgresQueueDriver from '../src/queueDriver/PostgresQueueDriver';

import { Client } from 'pg'

async function start() {

    const pgClient = new Client({
        host: process.env.POSTGRES_HOST,
        database: process.env.POSTGRES_DATABASE,
        password: process.env.POSTGRES_PASSWORD,
        port: Number(process.env.POSTGRES_PORT),
        user: process.env.POSTGRES_USER,
        ssl: false,
    })

    await pgClient.connect()


    const postgresQueueDriver = new PostgresQueueDriver(
        pgClient, "pgmq"
    )


    const consumer = new Consumer(
        {
            queueName: 'subscriptions',
            visibilityTime: 30,
            consumeType: "read",
            poolSize: 8,
            timeMsWaitBeforeNextPolling: 1000,
            enabledPolling: true,
            queueNameDlq: "subscriptions_dlq",
            totalRetriesBeforeSendToDlq: 2
        },
        async function (message: { [key: string]: any }, signal): Promise<void> {
            console.log(message)
            throw new Error("Error in message")
            // const url = "https://jsonplaceholder.typicode.com/todos/1";
            // await timersPromises.setTimeout(100, null, { signal });
            // console.log("Fetching data...");
            // const response = await fetch(url, { signal });
            // const todo = await response.json();
            // console.log("Todo:", todo);
        },
        postgresQueueDriver
    );

    consumer.on("send-to-dlq", (message: { [key: string]: any }) => {
        console.log("Send to DLQ =>", message)
    })

    consumer.on('error', (err: Error) => {
        console.error('Error consuming message:', err.message);
    });

    await consumer.start();

    process.on("SIGINT", async () => {
        await pgClient.end()
        process.exit(0)
    })

}

start()