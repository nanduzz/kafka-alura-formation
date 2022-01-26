import infra.KafkaDispatcher
import infra.KafkaService
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*

fun main() {
    BatchSendMessageService().main()
}

class BatchSendMessageService {

    private val url = "jdbc:sqlite:target/users_database.db"
    private val connection: Connection = DriverManager.getConnection(url)
    private val userDispatcher = KafkaDispatcher<User>()

    init {
        try {
            connection.createStatement().execute(
                """create table  Users(
                    uuid varchar (200) primary key,
                    email varchar (200))"""
            )
        } catch (e: SQLException) {
            // be carefull, this could be a real error
        }

    }

    fun main() {
        KafkaService(
            groupId = BatchSendMessageService::class.java.simpleName,
            topic = "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
            properties = mapOf(),
            consume = ::parse
        ).use {
            it.execute()
        }
    }


    private fun parse(record: ConsumerRecord<String, Message<String>>) {
        try {

            println("------------------")
            println("Processing new batch")
            val message = record.value()
            println("All Payload : ${record.value()}")
            println("Payload extracted: ${message.payload}")

            val users = getUsers()
            println("Users: ${users.size}")
            for (user in users) {
                userDispatcher.dispatchAsync(
                    message.payload,
                    user.id.toString(),
                    message.id.continueWith(BatchSendMessageService::class.java.simpleName),
                    user
                )
                println("Acho que enviei para o usuario ${user.id}")
            }
            println("Sent generate report to all users!")
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }


    private fun getUsers(): MutableList<User> {
        val results = connection.prepareStatement("select uuid from Users").executeQuery()
        val users = ArrayList<User>()
        while (results.next()) {
            val user = User(UUID.fromString(results.getString(1)))
            users.add(user)
        }

        return users
    }

}

private data class User(
    val id: UUID = UUID.randomUUID()
)

