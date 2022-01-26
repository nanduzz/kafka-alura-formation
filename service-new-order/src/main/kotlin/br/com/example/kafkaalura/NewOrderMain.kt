package br.com.example.kafkaalura

import infra.dispatcher.KafkaDispatcher
import model.CorrelationId
import java.math.BigDecimal
import java.util.*

fun main() {
    NewOrderMain().main()
}

class NewOrderMain {

    fun main() {
        KafkaDispatcher<Order>().use { orderDispatcher ->
            for (i in 0..1000) {
                val email = "${Math.random()}.@email.com"
                val orderId = UUID.randomUUID().toString()
                val amount = BigDecimal(Math.random() * 5000 + 1)
                val order = Order(orderId, amount, email)

                orderDispatcher.dispatch(
                    "ECOMMERCE_NEW_ORDER",
                    email,
                    CorrelationId(NewOrderMain::class.java.simpleName),
                    order
                )
            }
        }
    }
}

data class Order(
    val orderId: String,
    val amount: BigDecimal,
    val email: String,
)

