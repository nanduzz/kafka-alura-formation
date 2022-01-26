package infra

import com.google.gson.*
import model.CorrelationId
import model.Message
import org.apache.kafka.common.serialization.Serializer
import java.lang.reflect.Type

class GsonSerializer<T> : Serializer<T> {

    private val gson: Gson = GsonBuilder().registerTypeAdapter(Message::class.java, MessageAdapter<T>()).create()

    override fun serialize(p0: String?, obj: T): ByteArray {
        return gson.toJson(obj).toByteArray()
    }

}

class MessageAdapter<T> : JsonSerializer<Message<T>>, JsonDeserializer<Message<T>> {

    override fun serialize(message: Message<T>, type: Type, context: JsonSerializationContext): JsonElement {
        val obj = JsonObject()
        obj.addProperty("type", message.payload!!::class.java.name)
        obj.add("payload", context.serialize(message.payload))
        obj.add("correlationId", context.serialize((message.id)))
        return obj
    }

    override fun deserialize(jsonElement: JsonElement, type: Type, context: JsonDeserializationContext): Message<T> {
        val obj = jsonElement.asJsonObject
        val payloadType = obj.get("type").asString
        val correlationId: CorrelationId = context.deserialize(obj.get("correlationId"), CorrelationId::class.java)
        try {
            // maybe you want to use a "accept list"
            val payload = context.deserialize<T>(obj.get("payload"), Class.forName(payloadType))
            return Message(correlationId, payload)
        } catch (e: ClassNotFoundException) {
            // you might want to deal with this exception
            throw JsonParseException(e)
        }
    }


}
