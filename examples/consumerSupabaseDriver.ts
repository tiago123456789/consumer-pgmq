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

async function start() {
    for (let i = 0; i < 50; i++) {
        await supabase.rpc("send", {
            queue_name: "subscriptions",
            message: { "message": `Message triggered at ${Date.now()}` }
        });
    }
    console.log("Total messages sent: ", 50)

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
            try {
                console.log(message)
                throw new Error("Error in message")
            } catch (error: any) {
                throw error
            }
        },
        supabaseQueueDriver
    );

    // consumer.on('finish', (message: { [key: string]: any }) => {
    //     console.log('Consumed message =>', message);
    // });

    consumer.on("send-to-dlq", (message: { [key: string]: any }) => {
        console.log("Send to DLQ =>", message)
    })
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