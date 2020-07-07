package myapp.test.infrastructure.producer.serializers

import com.fasterxml.jackson.annotation.JsonProperty
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class JsonSerializerSchemaTest {

    class Book(val myField1: Int, val myField2: Double, myField3: String)

    fun buildProducer(): KafkaProducer<String, Book> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaJsonSchemaSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val sche = "\$schema"
        val id = "\$id"
        val schema = Schema.Parser().parse("""
            {
              "$sche": "http://json-schema.org/draft-07/schema#",
              "$id": "http://example.com/myURI.schema.json",
              "title": "value_jsons_serializer_schemaless",
              "description": "Sample schema to help you get started.",
              "type": "object",
              "properties": {
                "myField1": {
                  "type": "integer",
                  "description": "The integer type is used for integral numbers."
                },
                "myField2": {
                  "type": "number",
                  "description": "The number type is used for any numeric type, either integers or floating point numbers."
                },
                "myField3": {
                  "type": "string",
                  "description": "The string type is used for strings of text."
                }
              }
            }
        """.trimIndent())


        val book = Book(myField1 = 13, myField2 = 46.8, myField3 = "some text here")
        buildProducer().apply{
            send(ProducerRecord("topic-jsonserializer-schema", book))
            flush()
            close()
        }
    }
}
