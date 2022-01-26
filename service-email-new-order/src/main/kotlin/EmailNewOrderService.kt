import infra.consumer.KafkaService
import infra.dispatcher.KafkaDispatcher
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.regex.Pattern

fun main() {
    val fraudDetectorService = EmailNewOrderService()
    fraudDetectorService.main()
}

class EmailNewOrderService {

    private val emailDispatcher = KafkaDispatcher<Email>()

    fun main() {
        KafkaService(
            groupId = EmailNewOrderService::class.java.simpleName,
            topic = Pattern.compile("ECOMMERCE_NEW_ORDER"),
            properties = mapOf(),
            consume = ::parse
        ).use {
            it.execute()
        }
    }

    private fun parse(record: ConsumerRecord<String, Message<Order>>) {
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

}

data class Order(
    val email: String
)

data class Email(
    val subject: String,
    val body: String
)
