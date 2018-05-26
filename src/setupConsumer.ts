import * as Logger from 'bunyan'
import { ConsumerGroup, Message } from 'kafka-node'
import { queue, retry, ErrorCallback } from 'async'
import createLogger from './createLogger'

export type Handler = (message: Message) => Promise<void>

export type Message = Message

export type Logger = Logger

export interface Options {
  zkHost: string
  groupId: string
  topic: string
  concurrency?: number
  processor: Handler
  recyler?: Handler
  handlerRetry?: {
    times: number
    interval: number
  }
  logger?: Logger
}

const DEFAULT_RETRY_OPTIONS = { times: 3, interval: 500 }

async function tryRecycle(
  logger: Logger,
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
  logger: Logger,
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

async function defaultRecycle(logger: Logger, msg: Message): Promise<void> {
  logger.warn('message failed to process and no recyler available', msg)
}

export function setupConsumer(options: Options) {
  const logger = options.logger || createLogger()
  const concurrency = options.concurrency || 1
  const defaultRecycler = (msg: Message) => defaultRecycle(logger, msg)
  const recycler = options.recyler || defaultRecycler

  logger.info('setting up kafka consumer', options)

  logger.debug('setting up async queue')
  const pending = queue<Message, void>((msg, callback) => {
    const childLogger = logger.child({
      messageOffset: msg.offset,
      messageKey: msg.key
    })
    setImmediate(() =>
      tryProcess(childLogger, msg, options.processor, recycler, callback)
    )
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
