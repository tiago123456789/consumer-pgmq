import { config } from "dotenv"
config()

import Consumer from '../src/consumer';
import PostgresQueueDriver from '../src/queueDriver/PostgresQueueDriver';
import timersPromises from "node:timers/promises";
import knex from 'knex'

async function start() {
    const connection = knex({
        client: 'pg',
        connection: {
            host: process.env.POSTGRES_HOST,
            database: process.env.POSTGRES_DATABASE,
            password: process.env.POSTGRES_PASSWORD,
            port: Number(process.env.POSTGRES_PORT),
            user: process.env.POSTGRES_USER,
            ssl: false,
        }
    });

    const postgresQueueDriver = new PostgresQueueDriver(connection, "pgmq")

    const consumer = new Consumer(
        {
            queueName: 'subscriptions',
            visibilityTime: 15,
            consumeType: "read",
            poolSize: 4,
            timeMsWaitBeforeNextPolling: 1000
        },
        async function (message: { [key: string]: any }, signal): Promise<void> {
            try {
                console.log(message)
                const url = "https://jsonplaceholder.typicode.com/todos/1";
                await timersPromises.setTimeout(100, null, { signal });
                console.log("Fetching data...");
                const response = await fetch(url, { signal });
                const todo = await response.json();
                console.log("Todo:", todo);
            } catch (error: any) {
                if (error.name === "AbortError") {
                    console.log("Operation aborted");
                } else {
                    console.error("Error:", error);
                }
            }
        },
        postgresQueueDriver
    );

    consumer.on('finish', (message: { [key: string]: any }) => {
        console.log('Consumed message =>', message);
    });

    consumer.on("abort-error", (err) => {
        console.log("Abort error =>", err)
    })

    consumer.on('error', (err: Error) => {
        if (err.message.includes("TypeError: fetch failed")) {
            console.log(err)
            process.exit(1);
        }
        console.error('Error consuming message:', err.message);
    });

    consumer.start();

}

start()