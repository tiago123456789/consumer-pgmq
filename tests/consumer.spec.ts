import Consumer from '../src/consumer'
import { Message, QueueDriver, Options } from '../src/type'

describe('Consumer', () => {
    let queueDriver: jest.Mocked<QueueDriver>

    const message: Message = ({
        msg_id: 1,
        read_ct: 0,
        enqueued_at: new Date().toISOString(),
        vt: new Date().toISOString(),
        message: { foo: 'bar' },
    })

    beforeEach(() => {
        queueDriver = {
            get: jest.fn(),
            pop: jest.fn(),
            delete: jest.fn(),
            send: jest.fn(),
        }
        jest.useFakeTimers();
    })


    afterEach(() => {
        jest.clearAllMocks()
    })


    it('Should throw error if set dead letter queue and no set total retries before send to dlq', async () => {
        try {
            queueDriver.get.mockResolvedValueOnce({ data: [message], error: null })
            queueDriver.delete.mockResolvedValueOnce({ error: null })
            queueDriver.get.mockResolvedValueOnce({ data: [], error: null })

            const handler = jest.fn(async () => { })
            const consumer = new Consumer(
                {
                    queueName: 'q',
                    consumeType: 'read',
                    visibilityTime: 1,
                    poolSize: 1,
                    timeMsWaitBeforeNextPolling: 1,
                    enabledPolling: true,
                    queueNameDlq: 'q_dlq',
                },
                handler,
                queueDriver
            )

            const onFinish = jest.fn()
            consumer.on('finish', onFinish)
            await consumer.start()
        } catch (error: any) {
            expect(error).toBeInstanceOf(Error)
            expect(error.message).toBe('The option totalRetriesBeforeSendToDlq is required when queueNameDlq is set')
        }
    })


    it('Should send message to dlq if read count is greater than total retries before send to dlq', async () => {
        const messageToSendDlq = { ...message }
        messageToSendDlq.read_ct = 3
        messageToSendDlq.msg_id = 2
        queueDriver.get.mockResolvedValueOnce({ data: [messageToSendDlq], error: null })
        queueDriver.delete.mockResolvedValueOnce({ error: null })
        queueDriver.get.mockResolvedValueOnce({ data: [], error: null })
        queueDriver.send.mockResolvedValueOnce({ error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'read',
                visibilityTime: 1,
                poolSize: 1,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true,
                queueNameDlq: 'q_dlq',
                totalRetriesBeforeSendToDlq: 2
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)
        await consumer.start()

        expect(handler).toHaveBeenCalledTimes(0)
        expect(onFinish).toHaveBeenCalledTimes(0)
        expect(queueDriver.delete).toHaveBeenCalledTimes(1)
        expect(queueDriver.send).toHaveBeenCalledTimes(1)
        expect(queueDriver.send).toHaveBeenCalledWith(
            'q_dlq', messageToSendDlq.message, expect.any(AbortSignal)
        )
    })

    it('Should send 2 messages to dlq if read count is greater than total retries before send to dlq', async () => {
        const messageToSendDlq = { ...message }
        messageToSendDlq.read_ct = 3
        messageToSendDlq.msg_id = 2
        queueDriver.get.mockResolvedValueOnce({
            data: [
                messageToSendDlq, messageToSendDlq
            ], error: null
        })
        queueDriver.delete.mockResolvedValueOnce({ error: null })
        queueDriver.get.mockResolvedValueOnce({ data: [], error: null })
        queueDriver.send.mockResolvedValue({ error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'read',
                visibilityTime: 1,
                poolSize: 2,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true,
                queueNameDlq: 'q_dlq',
                totalRetriesBeforeSendToDlq: 2
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)
        await consumer.start()

        expect(handler).toHaveBeenCalledTimes(0)
        expect(onFinish).toHaveBeenCalledTimes(0)
        expect(queueDriver.delete).toHaveBeenCalledTimes(2)
        expect(queueDriver.send).toHaveBeenCalledTimes(2)
        expect(queueDriver.send).toHaveBeenCalledWith(
            'q_dlq', messageToSendDlq.message, expect.any(AbortSignal)
        )
    })


    it('Should send only 1 message to dlq if read count is greater than total retries before send to dlq', async () => {
        const messageToSendDlq = { ...message }
        messageToSendDlq.read_ct = 3
        messageToSendDlq.msg_id = 2

        const messageToSendDlq2 = { ...message }
        messageToSendDlq2.read_ct = 1
        messageToSendDlq2.msg_id = 3
        queueDriver.get.mockResolvedValueOnce({
            data: [
                messageToSendDlq, messageToSendDlq2
            ], error: null
        })
        queueDriver.delete.mockResolvedValue({ error: null })
        queueDriver.get.mockResolvedValueOnce({ data: [], error: null })
        queueDriver.send.mockResolvedValue({ error: null })

        const handler = jest.fn(async () => {
            return Promise.resolve()
        })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'read',
                visibilityTime: 1,
                poolSize: 2,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true,
                queueNameDlq: 'q_dlq',
                totalRetriesBeforeSendToDlq: 2
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)
        await consumer.start()

        expect(handler).toHaveBeenCalledTimes(1)
        expect(onFinish).toHaveBeenCalledTimes(1)
        expect(queueDriver.delete).toHaveBeenCalledTimes(2)
        expect(queueDriver.send).toHaveBeenCalledTimes(1)
        expect(queueDriver.send).toHaveBeenCalledWith(
            'q_dlq', messageToSendDlq.message, expect.any(AbortSignal)
        )
    })



    it('Should not process message if read method does not return any message second polling', async () => {
        queueDriver.get.mockResolvedValueOnce({ data: [message], error: null })
        queueDriver.delete.mockResolvedValueOnce({ error: null })
        queueDriver.get.mockResolvedValueOnce({ data: [], error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'read',
                visibilityTime: 1,
                poolSize: 1,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)

        await consumer.start()

        expect(handler).toHaveBeenCalledTimes(1)
        expect(onFinish).toHaveBeenCalledTimes(1)
        expect(queueDriver.delete).toHaveBeenCalledTimes(1)

    })

    it('Should not process message if pop method does not return any message second polling', async () => {
        queueDriver.pop.mockResolvedValueOnce({ data: [message], error: null })
        queueDriver.pop.mockResolvedValueOnce({ data: [], error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'pop',
                visibilityTime: 1,
                poolSize: 1,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)

        await consumer.start()

        expect(handler).toHaveBeenCalledTimes(1)
        expect(onFinish).toHaveBeenCalledTimes(1)
    })


    it('Should not process message if pop method does not return any message', async () => {
        queueDriver.pop.mockResolvedValue({ data: [], error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'pop',
                visibilityTime: 1,
                poolSize: 1,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)

        await consumer.start()

        expect(handler).not.toHaveBeenCalled()
        expect(onFinish).not.toHaveBeenCalled()
    })

    it('Should not process message if get method does not return any message', async () => {
        queueDriver.get.mockResolvedValue({ data: [], error: null })
        queueDriver.delete.mockResolvedValue({ error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'read',
                visibilityTime: 1,
                poolSize: 1,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)

        await consumer.start()

        expect(handler).not.toHaveBeenCalled()
        expect(queueDriver.delete).not.toHaveBeenCalled()
        expect(onFinish).not.toHaveBeenCalled()
    })


    it('Should process message, delete it, emit finish', async () => {
        const msg = message
        queueDriver.get.mockResolvedValue({ data: [msg], error: null })
        queueDriver.delete.mockResolvedValue({ error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'read',
                visibilityTime: 1,
                poolSize: 1,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)

        await consumer.start()

        expect(handler).toHaveBeenCalledWith(msg.message, expect.any(AbortSignal))
        expect(queueDriver.delete).toHaveBeenCalledWith('q', msg.msg_id)
        expect(onFinish).toHaveBeenCalledWith(msg)
    })


    it('Should process 4 messages using consumeType get', async () => {
        const msgs = [message, message, message, message]
        queueDriver.get.mockResolvedValue({ data: msgs, error: null })
        queueDriver.delete.mockResolvedValue({ error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            {
                queueName: 'q',
                consumeType: 'read',
                visibilityTime: 1,
                poolSize: 4,
                timeMsWaitBeforeNextPolling: 1,
                enabledPolling: true
            },
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)

        await consumer.start()

        expect(handler).toHaveBeenCalledWith(msgs[0].message, expect.any(AbortSignal))
        expect(queueDriver.delete).toHaveBeenCalledWith('q', msgs[0].msg_id)
        expect(onFinish).toHaveBeenCalledWith(msgs[0])
        expect(onFinish).toHaveBeenCalledTimes(4)
    })

    it('Should process message using consumeType is pop emits finish', async () => {
        const msg = message
        queueDriver.pop.mockResolvedValue({ data: [msg], error: null })

        const handler = jest.fn(async () => { })
        const consumer = new Consumer(
            { queueName: 'q', consumeType: 'pop', timeMsWaitBeforeNextPolling: 1 } as Options,
            handler,
            queueDriver
        )

        const onFinish = jest.fn()
        consumer.on('finish', onFinish)

        await consumer.start()

        expect(handler).toHaveBeenCalledWith(msg.message, expect.any(AbortSignal))
        expect(queueDriver.delete).not.toHaveBeenCalled()
        expect(onFinish).toHaveBeenCalledWith(msg)
        expect(queueDriver.delete).toHaveBeenCalledTimes(0)
    })

    it('Should emit error when consumeType is read without visibilityTime', async () => {
        const consumer = new Consumer(
            { queueName: 'q', consumeType: 'read' } as Options,
            async () => { },
            queueDriver
        )

        const onError = jest.fn()
        consumer.on('error', onError)

        await consumer.start()

        expect(onError).toHaveBeenCalled()
        expect((onError.mock.calls[0][0] as Error).message)
            .toBe('visibilityTime is required for read')
    })
})