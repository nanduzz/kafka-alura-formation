package br.com.example.kafkaalura

import infra.consumer.ConsumerService
import infra.consumer.KafkaService
import infra.consumer.ServiceRunner
import infra.dispatcher.KafkaDispatcher
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.regex.Pattern

fun main() {
    val fraudDetectorService = EmailNewOrderService()
    fraudDetectorService.main()
}

class EmailNewOrderService : ConsumerService<Order> {

    private val emailDispatcher = KafkaDispatcher<Email>()

    fun main() {
        ServiceRunner(::EmailNewOrderService).start(1)
    }

    override fun parse(record: ConsumerRecord<String, Message<Order>>) {
        println("------------------")
        println("Processing new order, preparing email")

        val emailCode = "Thank you for your order! We are processing it!"
        val message = record.value()
        val order = message.payload

        emailDispatcher.dispatch(
            topic = "ECOMMERCE_SEND_EMAIL",
            key = order.email,
            id = message.id.continueWith(EmailNewOrderService::class.java.simpleName),
            payload = Email(order.email, emailCode)
        )
    }

    override fun getTopic(): String {
        return "ECOMMERCE_NEW_ORDER"
    }

    override fun getConsumerGroup(): String {
        return "4" + EmailNewOrderService::class.java.simpleName
    }

}

data class Order(
    val email: String
)

data class Email(
    val subject: String,
    val body: String
)
