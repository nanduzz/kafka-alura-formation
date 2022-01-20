package infra

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
    private val consume: (record: ConsumerRecord<String, T>) -> Unit,
    private val type: Class<T>,
    private val properties: Map<String, String>
) : Closeable {

    private var consumer: KafkaConsumer<String, T> = KafkaConsumer<String, T>(properties(type, properties))

    constructor(
        groupId: String,
        topic: Pattern,
        type: Class<T>,
        properties: Map<String, String>,
        consume: (ConsumerRecord<String, T>) -> Unit
    ) : this(groupId = groupId, type = type, properties = properties, consume = consume) {
        consumer.subscribe(topic)
    }

    constructor(
        groupId: String,
        topic: String,
        type: Class<T>,
        properties: Map<String, String>,
        consume: (ConsumerRecord<String, T>) -> Unit
    ) : this(groupId = groupId, type = type, properties = properties, consume = consume) {
        consumer.subscribe(listOf(topic))
    }

    fun execute() {

        while (true) {
            val records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty) {
                println("Encontrei ${records.count()} registros")
            }
            for (record in records) {
                this.consume(record)
            }
        }
    }

    private fun properties(type: Class<T>, overrideProperties: Map<String, String>): Properties {
        val properties = Properties()

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.name)
        properties.putAll(overrideProperties)

        return properties
    }

    override fun close() {
        consumer.close()
    }
}
