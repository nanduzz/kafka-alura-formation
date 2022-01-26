import infra.dispatcher.KafkaDispatcher
import infra.consumer.KafkaService
import model.CorrelationId
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.util.regex.Pattern

fun main() {
    val fraudDetectorService = FraudDetectorService()
    fraudDetectorService.main()
}

class FraudDetectorService {

    private val orderDispatcher = KafkaDispatcher<Order>()

    fun main() {
        KafkaService(
            groupId = FraudDetectorService::class.java.simpleName,
            topic = Pattern.compile("ECOMMERCE_NEW_ORDER"),
            properties = mapOf(),
            consume = ::parse
        ).use {
            it.execute()
        }
    }

    private fun parse(record: ConsumerRecord<String, Message<Order>>) {
        println("------------------")
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

        val order = record.value().payload
        if (isFraud(order)) {
            //pretending that the fraud happens when the amount is >=400
            println("Order is a fraud!!!!")
            orderDispatcher.dispatch(
                "ECOMMERCE_ORDER_REJECTED",
                order.email,
                CorrelationId(FraudDetectorService::class.java.simpleName),
                order
            )
        } else {
            println("Approved: $order")
            orderDispatcher.dispatch(
                "ECOMMERCE_ORDER_APPROVED",
                order.email,
                CorrelationId(FraudDetectorService::class.java.simpleName),
                order
            )
        }
    }

    private fun isFraud(order: Order) = order.amount >= BigDecimal("4500")
}

data class Order(
    val email: String,
    val orderId: String,
    val amount: BigDecimal
)

