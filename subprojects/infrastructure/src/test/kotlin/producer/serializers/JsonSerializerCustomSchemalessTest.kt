package myapp.test.infrastructure.producer.serializers

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.io.IOException
import java.io.StringWriter
import java.util.*

data class User(val firstName: String, val lastName: String, val age: Int)

class UserSerializer: Serializer<User> {
    override fun configure(map: Map<String?, *>?, b: Boolean) {}
    override fun close() {}
    override fun serialize(arg0: String?, arg1: User?): ByteArray? {
        val data = """
            {
                "firstName":"${arg1!!.firstName}",
                "lastName":"${arg1!!.lastName}",
                "age":${arg1!!.age}
            }""".trimIndent()
        return data.toByteArray()
    }
}

class JsonProducerSchemalessTest {

    fun buildProducer(): KafkaProducer<String, User> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", UserSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val user = User(firstName = "anotherone", lastName = "lastname here", age = (0 until 10000).random())

        buildProducer().apply{
            send(ProducerRecord("topic-custom-serializer7", user)).get()
            flush()
            close()
        }
    }
}
