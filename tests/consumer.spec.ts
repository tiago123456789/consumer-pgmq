import Consumer from '../src/consumer'
import { Message, QueueDriver, Options } from '../src/type'
import timersPromises from 'node:timers/promises'

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
        }
        jest.useFakeTimers();
    })


    afterEach(() => {
        jest.clearAllMocks()
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
                timeMsWaitBeforeNextPolling: 1
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
                timeMsWaitBeforeNextPolling: 1
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
                timeMsWaitBeforeNextPolling: 1
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
                timeMsWaitBeforeNextPolling: 1
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
                timeMsWaitBeforeNextPolling: 1
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
                timeMsWaitBeforeNextPolling: 1
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