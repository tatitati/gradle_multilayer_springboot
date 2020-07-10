package myapp.test.infrastructure.producer.serializers.jsonserializers

import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class KafkaJsonSerializerTest {

    class Book(val title: String, val authorName: String)

    fun buildProducer(): KafkaProducer<String, Book> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaJsonSerializer::class.java) // this doesnt create any schema
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val record = Book(title = "jungle book2 ", authorName = "tupapa")

        buildProducer().apply{
            send(ProducerRecord("topic-kafkajsonsserializer", record)).get()
            flush()
            close()
        }
    }
}
