package infra.consumer

import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord

interface ConsumerService<T> {
    fun getTopic(): String
    fun parse(record: ConsumerRecord<String, Message<T>>)
    fun getConsumerGroup(): String
}
