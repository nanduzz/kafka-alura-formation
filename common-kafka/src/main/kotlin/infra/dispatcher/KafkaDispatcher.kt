package infra.dispatcher

import model.CorrelationId
import model.Message
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.util.*
import java.util.concurrent.Future

class KafkaDispatcher<T> : Closeable {
    private val producer: KafkaProducer<String, Message<T>>

    init {
        this.producer = KafkaProducer<String, Message<T>>(properties())
    }

    private fun properties(): Properties {
        val properties = Properties()

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1  :9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer::class.java.name)

        return properties
    }

    fun dispatchAsync(topic: String, key: String, id: CorrelationId, payload: T): Future<RecordMetadata> {
        val value = Message(id.continueWith("_$topic"), payload)
        val record = ProducerRecord(topic, key, value)
        return producer.send(record) { data, ex ->
            if (ex != null) {
                println(ex.printStackTrace());
                return@send
            }
            println("Sucesso enviando ${data.topic()}:::partition ${data.partition()} / offset ${data.offset()}")
        }
    }

    fun dispatch(topic: String, key: String, id: CorrelationId, payload: T) {
        val future = dispatchAsync(topic, key, id, payload)
        future.get()
    }


    override fun close() {
        println("========= Closing Dispatcher =========")
        producer.close()
    }

}
