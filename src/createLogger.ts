import * as Logger from 'bunyan'

export default function(): Logger {
  return Logger.createLogger({
    name: 'tamed-kafka',
    level: process.env.LOG_LEVEL as Logger.LogLevel
  })
}
