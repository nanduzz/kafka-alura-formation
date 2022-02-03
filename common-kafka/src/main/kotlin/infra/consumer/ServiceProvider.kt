package infra.consumer

import java.util.concurrent.Callable

class ServiceProvider<T>(
    val create: () -> ConsumerService<T>
) : Callable<Void> {

    override fun call(): Void? {
        val service = create()
        KafkaService(
            groupId = service.getConsumerGroup(),
            topic = service.getTopic(),
            properties = mapOf(),
            consume = service::parse
        ).execute()

        return null
    }
}
