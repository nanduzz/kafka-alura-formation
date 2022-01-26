import infra.dispatcher.KafkaDispatcher
import model.CorrelationId

fun main() {
    FakeHttpService().main()
}

class FakeHttpService {
    fun main() {

        KafkaDispatcher<String>().use {
            val value = "ECOMMERCE_USER_GENERATE_READING_REPORT"
            it.dispatch(
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                value,
                CorrelationId(FakeHttpService::class.java.simpleName),
                value
            )
        }
    }
}
