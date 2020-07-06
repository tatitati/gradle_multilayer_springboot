package myapp.test.infrastructure.producer.serializers

import com.fasterxml.jackson.annotation.JsonProperty
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*


data class Book(
        @JsonProperty("title") var title: String,
        @JsonProperty("author") var authorName: String)


class JsonPropertiesTest {

    fun buildProducer(): KafkaProducer<String, Book> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaJsonSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer<String, Book>(
                properties
        )
    }

    // you can test the producer with the CLI:
    // kafka-avro-console-consumer --topic my-generic-record-value --bootstrap-server $khost --property schema.registry.url=http://127.0.0.1:8081
    @Test
    fun jsonProducer(){
        val topic = "book-topic-value"
        val record = Book(title = "jungle book", authorName = "tupapa")

        buildProducer().apply{
            send(ProducerRecord(topic, record))
            flush()
            close()
        }
    }
}
