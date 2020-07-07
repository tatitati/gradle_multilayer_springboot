package myapp.test.infrastructure.producer.serializers

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
              "name": "person",
              "namespace": "myapp.infrastructure",
              "fields": [
                {"name": "firstName","type": "string"},
                {"name": "lastName","type": "string"},
                {"name": "age","type": "int"}
              ]
            }
        """.trimIndent())

        val avroRecord: GenericRecord = GenericRecordBuilder(schema).apply{
            set("firstName", "sam")
            set("lastName", "dedios")
            set("age", 66)
        }.build()

        buildProducer().apply{
            send(ProducerRecord("topic-withavrorecord", avroRecord))
            flush()
            close()
        }
    }
}