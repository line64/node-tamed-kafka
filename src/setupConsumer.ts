import * as bunyan from 'bunyan'
import { ConsumerGroup, Message } from 'kafka-node'
import { queue, retry, ErrorCallback } from 'async'

export interface Options {
  zkHost: string
  groupId: string
  topic: string
  concurrency?: number
  handlerRetry?: {
    times: number
    interval: number
  }
}

export type Handler = (message: Message) => Promise<void>

const DEFAULT_RETRY_OPTIONS = { times: 3, interval: 500 }

async function tryRecycle(
  logger: bunyan,
  msg: Message,
  recyclebin: Handler
): Promise<void> {
  try {
    logger.info('message recyle started')
    await recyclebin(msg)
    logger.info('message recycle succesful')
  } catch (err) {
    logger.error('message recyle failed, dropping message', err)
    logger.trace('message payload that failed to recyle', msg)
  }
}

async function tryProcess(
  logger: bunyan,
  msg: Message,
  worker: Handler,
  recyle: Handler,
  callback: ErrorCallback<void>
): Promise<void> {
  try {
    logger.info('message handling started')
    await worker(msg)
    logger.info('message handling successful')
  } catch (err) {
    logger.warn('message handle failed, redirecting to recyclebin', err)
    logger.trace('message handle failed payload', msg)
    await tryRecycle(logger, msg, recyle)
  } finally {
    logger.debug('message handle finished, calling callback')
    callback()
  }
}

export function setupConsumer(
  logger: bunyan,
  options: Options,
  process: Handler,
  recycle: Handler
) {
  logger.info('setting up kafka consumer', options)

  logger.debug('setting up async queue')
  const concurrency = options.concurrency || 1
  const pending = queue<Message, void>((msg, callback) => {
    logger = logger.child({ messageOffset: msg.offset, messageKey: msg.key })
    setImmediate(() => tryProcess(logger, msg, process, recycle, callback))
  }, concurrency)

  logger.debug('creating kafka-node consumer group', options)
  const consumer = new ConsumerGroup(
    {
      host: options.zkHost,
      groupId: options.groupId,
      fromOffset: 'earliest'
    },
    options.topic
  )

  consumer.on('error', err => {
    logger.error(err)
  })

  consumer.on('message', msg => {
    logger.debug('consumer received message, pausing consumption')
    consumer.pause()
    logger.debug('pushing message to pending queue')
    pending.push(msg)
  })

  pending.drain = () => {
    logger.warn('pending queue drained, resuming')
    consumer.resume()
  }

  logger.info('kafka-consumer setup completed, waiting for messages')
}
