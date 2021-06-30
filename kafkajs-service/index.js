import { Kafka } from 'kafkajs'

const topic = "exp-queue_general-5"

const getTime = () => {
	let date = new Date()

	return `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
}

const kafka = new Kafka({
	clientId: 'kafkajs',
	brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({
	groupId: topic
})

await Promise.all([
	producer.connect(),
	consumer.connect(),
	consumer.subscribe({ topic: `${topic}-forth`, fromBeginning: true })
])

await consumer
	.run({
		eachMessage: async ({ partition, message }) => {
			// let time = Date.now()

			// console.log({
			// 	partition,
			// 	offset: message.offset,
			// 	value: message.value.toString()
			// })

			// ? For testing race condition
			// await new Promise(resolve => setTimeout(resolve, 3000))

			await producer.send({
				topic: `${topic}-back`,
				messages: [
					{
						key: message.key,
						value: getTime()
					}
				]
			})

			// console.log(`Take ${Date.now() - time}`)
		}
	})
	.catch((err) => {
		console.warn('Err:', err)
	})