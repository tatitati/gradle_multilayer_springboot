package myapp.test.infrastructure.producer.serializers

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.Test
import java.util.*

data class User(val firstName: String, val lastName: String, val age: Int)

class UserSerializer: Serializer<User> {
    override fun configure(map: Map<String?, *>?, b: Boolean) {}
    override fun close() {}
    override fun serialize(arg0: String?, arg1: User?): ByteArray? {
        var retVal: ByteArray? = null
        val objectMapper = ObjectMapper()
        try {
            retVal = objectMapper.writeValueAsString(arg1).toByteArray()
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return retVal
    }
}

class JsonProducerTest {

    fun buildProducer(): KafkaProducer<String, User> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", UserSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer(properties)
    }

    // you can test the producer with the CLI:
    // kafka-avro-console-consumer --topic my-generic-record-value --bootstrap-server $khost --property schema.registry.url=http://127.0.0.1:8081
    @Test
    fun jsonProducer(){
        val topic = "my-json_serielizer-topic"
        val user = User(firstName = "namehere", lastName = "lastname here", age = (0 until 10000).random())

        buildProducer().apply{
            send(ProducerRecord(topic, user))
            flush()
            close()
        }
    }
}
