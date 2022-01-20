import infra.KafkaService
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.util.regex.Pattern

fun main() {
    val fraudDetectorService = FraudDetectorService()
    fraudDetectorService.main()
}


class FraudDetectorService {

    fun main() {
        KafkaService(
            groupId = FraudDetectorService::class.java.simpleName,
            topic = Pattern.compile("ECOMMERCE_NEW_ORDER"),
            type = Order::class.java,
            properties = mapOf(),
            consume = ::parse
        ).use {
            it.execute()
        }
    }

    private fun parse(record: ConsumerRecord<String, Order>) {
        println("Processing new order, checking for fraud")
        println(record.key())
        println(record.value())
        println(record.partition())
        println(record.offset())

        try {
            Thread.sleep(1000)
        } catch (e: InterruptedException) {
            // ignoring
            e.printStackTrace()
        }
    }
}

data class Order(
    val userId: String,
    val orderId: String,
    val amount: BigDecimal
)

