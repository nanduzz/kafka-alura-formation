import infra.consumer.KafkaService
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord

fun main() {
    val emailService = EmailService()
    emailService.main()
}

class EmailService {

    fun main() {
        KafkaService(
            groupId = EmailService::class.java.simpleName,
            topic = "ECOMMERCE_SEND_EMAIL",
            properties = mapOf(),
            consume = ::parse
        ).use {
            it.execute()
        }
    }

    private fun parse(record: ConsumerRecord<String, Message<Email>>) {
        println("Processing new order, checking for fraud")
        println(record.key())
        println(record.value())
        println(record.partition())
        println(record.offset())

        try {
            Thread.sleep(100)
        } catch (e: InterruptedException) {
            // ignoring
            e.printStackTrace()
        }
    }
}

data class Email(
    val subject: String,
    val body: String
)

