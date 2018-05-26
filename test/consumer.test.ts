import { setupConsumer } from '../src/tamed-kafka'

describe('Setup Consumer', () => {
  it('fails when trying to connect to non-existing kafka', () => {
    setupConsumer({
      groupId: 'test-consumer',
      processor: async msg => console.log(msg),
      topic: 'non-existing-topic',
      zkHost: 'badhost:2818'
    })
  })
})
