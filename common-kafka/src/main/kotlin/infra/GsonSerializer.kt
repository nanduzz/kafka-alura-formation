package infra

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serializer

class GsonSerializer<T> : Serializer<T> {

    private val gson: Gson = GsonBuilder().create()

    override fun serialize(p0: String?, obj: T?): ByteArray {
        return gson.toJson(obj).toByteArray()
    }
}
