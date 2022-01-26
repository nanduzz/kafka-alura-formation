import infra.consumer.KafkaService
import model.Message
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.regex.Pattern

fun main() {
    LogService().main()
}

class LogService {

    fun main() {
        KafkaService(
            groupId = LogService::class.java.simpleName,
            topic = Pattern.compile("ECOMMERCE.*"),
            properties = mapOf(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name),
            consume = ::parse
        ).use {
            it.execute()
        }
    }

    private fun parse(record: ConsumerRecord<String, Message<String>>) {
        println("==========LOG==============")
        println(record.topic())
        println(record.key())
        println(record.value())
        println(record.partition())
        println(record.offset())
    }
}
