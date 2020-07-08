package myapp.test.infrastructure.producer.serializers.avroserializers

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class AvroSerializerSchemaTest {

    fun buildProducer(): KafkaProducer<String, GenericRecord> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaAvroSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val schema = Schema.Parser().parse("""
            {
              "type": "record",
              "namespace": "com.mycorp.mynamespace",
              "name": "value_jsons_serializer_schemaless",
              "doc": "Sample schema to help you get started.",
              "fields": [
                {
                  "name": "myField1",
                  "type": "int",
                  "doc": "The int type is a 32-bit signed integer."
                },
                {
                  "name": "myField2",
                  "type": "double",
                  "doc": "The double type is a double precision (64-bit) IEEE 754 floating-point number."
                },
                {
                  "name": "myField3",
                  "type": "string",
                  "doc": "The string is a unicode character sequence."
                }
              ]
            }
        """.trimIndent())

        val avroRecord: GenericRecord = GenericRecordBuilder(schema).apply{
            set("myField1", 33)
            set("myField2", 3.5)
            set("myField3", "some text here")
        }.build()

        buildProducer().apply{
            send(ProducerRecord("topic-withavrorecord-schema", avroRecord)).get()
            flush()
            close()
        }
    }
}
