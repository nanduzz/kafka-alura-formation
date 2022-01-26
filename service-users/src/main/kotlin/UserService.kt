import infra.KafkaService
import model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*
import java.util.regex.Pattern

fun main() {
    CreateUserService().main()
}

class CreateUserService() {

    private val url = "jdbc:sqlite:target/users_database.db"
    private val connection: Connection = DriverManager.getConnection(url)

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
            groupId = CreateUserService::class.java.simpleName,
            topic = Pattern.compile("ECOMMERCE_NEW_ORDER"),
            properties = mapOf(),
            consume = ::parse
        ).use {
            it.execute()
        }
    }

    private fun parse(record: ConsumerRecord<String, Message<Order>>) {
        println("------------------")
        println("Processing new order, checking for new user")

        val order = record.value().payload
        println(order)

        if (isNewUser(order.email)) {
            insertNewUser(order.email)
        }

    }

    private fun insertNewUser(email: String) {
        val insert = connection.prepareStatement("insert into Users(uuid,email) values(?,?)")
        insert.setString(1, UUID.randomUUID().toString())
        insert.setString(2, email)
        insert.execute()
        println("Usuário uuid e $email adicionado")
    }

    private fun isNewUser(email: String): Boolean {
        val exists = connection.prepareStatement("select uuid from Users where email = ? limit 1")
        exists.setString(1, email)
        val results = exists.executeQuery()
        return !results.next()
    }

}

data class Order(
    val orderId: String,
    val amount: BigDecimal,
    val email: String
)
