import infra.consumer.ConsumerService
import infra.consumer.ServiceRunner
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File
import java.util.*

fun main() {
    ReadingReportService().main()
}

class ReadingReportService : ConsumerService<User> {

    companion object {
        val SOURCE = File("src/main/resources/report.txt")
    }

    fun main() {
        ServiceRunner(::ReadingReportService).start(3)
    }

    override fun parse(record: ConsumerRecord<String, Message<User>>) {
        try {
            println("------------------")
            println("Processing report for ${record.value()}")
            val user = record.value().payload
            val target = File(user.getReportPath())

            IO.copyTo(SOURCE, target)
            IO.append(target, "created for ${user.id}")

            println("File created: ${target.absolutePath}")
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    override fun getTopic(): String {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT"
    }

    override fun getConsumerGroup(): String {
        return ReadingReportService::class.java.simpleName
    }

}

data class User(
    val id: UUID = UUID.randomUUID()
) {
    fun getReportPath(): String {
        return "target/${this.id}.report.txt"
    }
}
