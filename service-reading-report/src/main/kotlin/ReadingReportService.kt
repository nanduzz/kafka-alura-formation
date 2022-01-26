import infra.KafkaService
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File
import java.util.*

fun main() {
    ReadingReportService().main()
}

class ReadingReportService {

    companion object{
        val SOURCE = File("src/main/resources/report.txt")
    }

    fun main() {
        KafkaService(
            groupId = ReadingReportService::class.java.simpleName,
            topic = "ECOMMERCE_USER_GENERATE_READING_REPORT",
            properties = mapOf(),
            consume = ::parse
        ).use {
            it.execute()
        }
    }

    private fun parse(record: ConsumerRecord<String, Message<User>>) {
        try {
            println("------------------")
            println("Processing report for ${record.value()}")
            val user = record.value().payload
            val target = File(user.getReportPath())

            IO.copyTo(SOURCE, target)
            IO.append(target, "created for ${user.id}")

            println("File created: ${target.absolutePath}")
        }catch (e : Exception){
            e.printStackTrace()
        }
    }

}

data class User(
    val id: UUID = UUID.randomUUID()
) {
    fun getReportPath(): String {
        return "target/${this.id}.report.txt"
    }
}
