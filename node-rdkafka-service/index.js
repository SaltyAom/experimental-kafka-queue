import Kafka from 'node-rdkafka'
const { Producer, KafkaConsumer } = Kafka

const topic = 'exp-queue_general-5'

const getTime = () => {
	let date = new Date()

	return `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
}

const producer = new Producer({
	'client.id': 'node-rdkafka',
	'metadata.broker.list': 'localhost:9092',
	'linger.ms': 40,
	'queue.buffering.max.messages': 1000000,
	'queue.buffering.max.ms': 50,
	'compression.codec': 'lz4',
	'batch.num.messages': 40000,
	retries: 0,
})

const consumer = new KafkaConsumer({
	'group.id': `${topic}-forth`,
	'metadata.broker.list': 'localhost:9092',
	'queued.min.messages': 200000,
	'fetch.error.backoff.ms': 250,
	"socket.blocking.max.ms": 500
})

producer.connect()
consumer.connect()

await Promise.all([
	new Promise((resolve) => {
		producer.on('ready', resolve)
	}),
	new Promise((resolve) => {
		consumer.on('ready', () => {
			consumer.subscribe([`${topic}-forth`]).consume()

			resolve()
		})
	})
])

console.log('Connected')

consumer.on('data', async ({ partition, value, key, offset }) => {
	let time = Date.now()
	let message = value.toString()

	// console.log({
	// 	partition,
	// 	offset,
	// 	message
	// })

	// ? For testing race condition
	// await new Promise(resolve => setTimeout(resolve, 3000))

	try {
		await producer.produce(
			`${topic}-back`,
			-1,
			Buffer.from(getTime()),
			key,
			Date.now()
		)
	} catch (error) {
		console.warn(error)
	}

	console.log(`Take ${Date.now() - time}`)
})
