# Tamed Kafka

A node kafka client tamed for everyday use

## Features

- wrap your message-handling logic in one big async function
- automatic message-handling concurrency option
- automatic message-handling retry option
- 'recycle' option for messages that failed during handling

## Usage

```node
async function handle(msg) => {
    //DO HEAVY ASYNC STUFF HERE FOR EACH MESSAGE
};

async function recycle(msg) => {
    //YOUR PROCESS FAILED, THIS IS YOUR LAST CHANCE TO TRACK
    //YOUR FAILED MESSAGE. THE CONSUMER WILL CONTINUE TO PROCESS
    //OTHER MESSAGES EITHER WAY
}

await setupConsumer({
    zkHost: config.ZOOKEEPER_CONNECTION,
    topic: "interesting-events",
    groupId: "my-consumer-1"
    handler: handle,
    recycler: recycle,
    concurrency: 1
});

console.log("processing events!")
```