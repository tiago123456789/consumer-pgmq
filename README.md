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


