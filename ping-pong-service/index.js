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
	groupId: 'experiment-kafka-queue-5'
})

await Promise.all([
	producer.connect(),
	consumer.connect(),
	consumer.subscribe({ topic: 'exp-queue_general-5-forth', fromBeginning: true })
])

await consumer
	.run({
		eachMessage: async ({ partition, message }) => {
			let time = Date.now()

			// console.log({
			// 	partition,
			// 	offset: message.offset,
			// 	value: message.value.toString()
			// })

			// ? For testing race condition
			// await new Promise(resolve => setTimeout(resolve, 3000))

			await producer.send({
				topic: 'exp-queue_general-5-back',
				messages: [
					{
						key: message.key,
						value: getTime()
					}
				]
			})

			console.log(`Take ${Date.now() - time}`)
		}
	})
	.catch((err) => {
		console.warn('Err:', err)
	})