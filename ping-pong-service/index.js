import { Kafka } from 'kafkajs'

const getTime = () => {
	let date = new Date()

	return `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
}

const kafka = new Kafka({
	clientId: 'Prism',
	brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({
	groupId: 'experiment-kafka-queue-4'
})

await Promise.all([
	producer.connect(),
	consumer.connect(),
	consumer.subscribe({ topic: 'exp-queue_general-4-forth', fromBeginning: true })
])

await consumer
	.run({
		eachMessage: async ({ partition, message }) => {
			let t = Date.now()

			console.log({
				partition,
				offset: message.offset,
				value: message.value.toString()
			})

			// ? For testing race condition
			await new Promise(resolve => setTimeout(resolve, 5000))

			await producer.send({
				topic: 'exp-queue_general-4-back',
				messages: [
					{
						key: message.key,
						value: getTime()
					}
				]
			})

			// console.log(`Take ${Date.now() - t}`)
		}
	})
	.catch((err) => {
		console.warn('Err:', err)
	})