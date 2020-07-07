package myapp.test.infrastructure.producer.serializers

import com.fasterxml.jackson.annotation.JsonProperty
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class JsonSerializerSchemaTest {

    class Book(val title: String, val authorName: String)

    fun buildProducer(): KafkaProducer<String, Book> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaJsonSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val record = Book(title = "jungle book2 ", authorName = "tupapa")

        val avroRecord: GenericRecord = GenericRecordBuilder(schema).apply{
            set("firstName", "sam")
            set("lastName", "dedios")
            set("age", 66)
        }.build()

        buildProducer().apply{
            send(ProducerRecord(topic, record))
            flush()
            close()
        }
    }
}
