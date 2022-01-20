package infra

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer

class GsonDeserializer<T> : Deserializer<T> {

    private var type: Class<T>? = null

    companion object {
        const val TYPE_CONFIG: String = "com.demo.infra.type_config"
    }

    private val gson: Gson = GsonBuilder().create()

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        super.configure(configs, isKey)
        val typeName = configs?.get(TYPE_CONFIG).toString()
        try {
            type = Class.forName(typeName) as Class<T>
        } catch (e: ClassNotFoundException) {
            throw RuntimeException("Type for deserialization does not exist in Classpath", e)
        }
    }

    override fun deserialize(p0: String?, bytes: ByteArray?): T {
        val data = bytes?.let { String(it) }
        return gson.fromJson(data, type)
    }

}
