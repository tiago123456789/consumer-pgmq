## About

This project is a consumer of Supabase/Postgresql queue(using pgmq extension) to simplify the process of consuming messages.

## Features

- Consumer message from Supabase queue. PS: instructions to setup https://supabase.com/blog/supabase-queues
- Consumer message from Postgresql queue. PS: instructions to setup https://github.com/pgmq/pgmq
- Support for both read and pop consume types
   - Read consume type is when the consumer gets the message and the message is not deleted from queue until the callback is executed with success.
   - Pop consume type is when the consumer gets the message and the message is deleted from queue.
- Support for both Supabase and Postgresql
- Support for both visibility time and pool size

## Installation

- Using pnpm
```bash
pnpm install consumer-pgmq
```

- Using npm
```bash
npm install consumer-pgmq
```
- Using yarn
```bash
yarn add consumer-pgmq
```

## Examples how to use

- Consuming messages from Supabase queue
```javascript
import { config } from "dotenv"
config()

import Consumer from '../src/consumer';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import SupabaseQueueDriver from '../src/queueDriver/SupabaseQueueDriver';


const supabase = createClient(
    // @ts-ignore
    process.env.SUPABASE_URL,
    process.env.SUPABASE_ANON_KEY,
    {
        db: {
            schema: 'pgmq_public'
        }
    }
);

const supabaseQueueDriver = new SupabaseQueueDriver(
    supabase as unknown as SupabaseClient
)


import timersPromises from "node:timers/promises";

async function start() {
    for (let i = 0; i < 200; i++) {
        await supabase.rpc("send", {
            queue_name: "subscriptions",
            message: { "message": `Message triggered at ${Date.now()}` }
        });
    }
    console.log("Total messages sent: ", 200)

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
        supabaseQueueDriver
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
```

- Consuming messages from Postgresql queue
```javascript
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
            ssl: false
        }
    });

    const postgresQueueDriver = new PostgresQueueDriver(connection)

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
```




