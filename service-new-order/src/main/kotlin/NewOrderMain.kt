import infra.KafkaDispatcher
import java.math.BigDecimal
import java.util.*

fun main() {
    NewOrderMain().main()
}

class NewOrderMain {

    fun main() {
        val emailDispatcher = KafkaDispatcher<Email>()
        KafkaDispatcher<Order>().use {
            for (i in 0..10) {
                val userId = UUID.randomUUID().toString()
                val orderId = UUID.randomUUID().toString()
                val amount = BigDecimal(Math.random() * 5000 + 1)
                val order = Order(userId, orderId, amount)

                it.dispatch("ECOMMERCE_NEW_ORDER", userId, order)

                val email = "Thank you for your order! We are processing it!"
                emailDispatcher.dispatch("ECOMMERCE_SEND_EMAIL", email, Email(email, email))
            }
        }

    }
}

data class Email(
    val subject: String,
    val body: String
)

data class Order(
    val userId: String,
    val orderId: String,
    val amount: BigDecimal
)

