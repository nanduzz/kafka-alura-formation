import infra.consumer.ConsumerService
import infra.consumer.ServiceRunner
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord

fun main() {
    val emailService = EmailService()
    emailService.main()
}

class EmailService : ConsumerService<Email> {

    companion object {
        const val THREADS = 5
    }

    fun main() {
        ServiceRunner(::EmailService).start(THREADS)
    }

    override fun getConsumerGroup(): String {
        return EmailService::class.java.simpleName
    }

    override fun getTopic(): String {
        return "ECOMMERCE_SEND_EMAIL"
    }

    override fun parse(record: ConsumerRecord<String, Message<Email>>) {
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

