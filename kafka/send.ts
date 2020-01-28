import { KafkaClient, Producer } from 'kafka-node'

// 0. Config
const config = {
    kafka: {
        connectTimeout: 5000,
        kafkaHost: 'localhost:9092',
    },
    kafkaProducer: {
        partitionerType: 2,
    },
}
// 1. Init kafka
const client = new KafkaClient(config.kafka);
const producer = new Producer(client, config.kafkaProducer);

client.on('ready', () => {
    console.log('kafka ready')
});

producer.on('error', function (err) {
    console.log(`producer errored ${err}`)
});

// 2. Send payload after producer ready
producer.on('ready', async () => {
    console.log('producer ready, sending data')
    const payloads = [
        { topic: 'test', messages: 'hello dude', partition: 0 },
        { topic: 'test', messages: ['helloP1212', 'world!!'] }
    ];
    const res = await sendSomePayload(payloads)
    console.log(res)
    process.exit()
});

async function sendSomePayload(payloads) {
    return new Promise((resolve, reject) => {
        producer.send(payloads, function (err, data) {
            if (err) {
                reject(`producer sending error ${err}`)
            }
            resolve(data)
        });
    })
}