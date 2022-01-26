package br.com.example.kafkaalura

import infra.KafkaDispatcher
import model.CorrelationId
import java.math.BigDecimal
import java.util.*

fun main() {
    NewOrderMain().main()
}

class NewOrderMain {

    fun main() {
        val emailDispatcher = KafkaDispatcher<Email>()
        KafkaDispatcher<Order>()
            .use {

                for (i in 0..1000) {
                    val email = "${Math.random()}.@email.com"
                    val orderId = UUID.randomUUID().toString()
                    val amount = BigDecimal(Math.random() * 5000 + 1)
                    val order = Order(orderId, amount, email)

                    it.dispatch(
                        "ECOMMERCE_NEW_ORDER",
                        email,
                        CorrelationId(NewOrderMain::class.java.simpleName),
                        order
                    )

                    val emailCode = "Thank you for your order! We are processing it!"
                    emailDispatcher.dispatch(
                        "ECOMMERCE_SEND_EMAIL",
                        email,
                        CorrelationId(NewOrderMain::class.java.simpleName),
                        Email(email, emailCode)
                    )
                }
            }
    }
}

data class Email(
    val subject: String,
    val body: String
)

data class Order(
    val orderId: String,
    val amount: BigDecimal,
    val email: String,
)

