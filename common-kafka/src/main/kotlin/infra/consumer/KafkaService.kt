package infra.consumer

import infra.dispatcher.GsonSerializer
import infra.dispatcher.KafkaDispatcher
import model.Message
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.io.Closeable
import java.time.Duration
import java.util.*
import java.util.regex.Pattern

class KafkaService<T> private constructor(
    private val groupId: String,
    private val consume: (ConsumerRecord<String, Message<T>>) -> Unit,
    private val properties: Map<String, String>
) : Closeable {

    private var consumer: KafkaConsumer<String, Message<T>> = KafkaConsumer<String, Message<T>>(properties(properties))

    constructor(
        groupId: String,
        topic: Pattern,
        properties: Map<String, String>,
        consume: (ConsumerRecord<String, Message<T>>) -> Unit
    ) : this(groupId = groupId, consume = consume, properties = properties) {
        consumer.subscribe(topic)
    }

    constructor(
        groupId: String,
        topic: String,
        properties: Map<String, String>,
        consume: (ConsumerRecord<String, Message<T>>) -> Unit
    ) : this(groupId = groupId, consume = consume, properties = properties) {
        consumer.subscribe(listOf(topic))
    }

    fun execute() {
        KafkaDispatcher<String>().use { deadLetter ->
            while (true) {
                val records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty) {
                    println("Encontrei ${records.count()} registros")
                }
                for (record in records) {
                    try {
                        if (Math.random() > 0.8){
                            throw RuntimeException("Causing exception to test deadletter")
                        }
                        this.consume(record)
                    } catch (e: RuntimeException) {
                        //so far, just logging
                        e.printStackTrace()
                        val message: Message<*> = record.value()
                        deadLetter.dispatch(
                            "ECOMMERCE_DEADLETTER",
                            message.id.toString(),
                            message.id.continueWith("DeadLetter"),
                            GsonSerializer<Message<*>>().serialize("", message).toString()
                        )
                    }
                }
            }
        }
    }

    private fun properties(overrideProperties: Map<String, String>): Properties {
        val properties = Properties()

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        properties.putAll(overrideProperties)

        return properties
    }

    override fun close() {
        consumer.close()
    }
}
