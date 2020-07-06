package myapp.test.infrastructure.schemaregistry

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class ProducerAvroWithSchemasTest {
    fun buildProducer(): KafkaProducer<String, GenericRecord> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaAvroSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer<String, GenericRecord>(properties)
    }

    // you can test the producer with the CLI:
    // kafka-avro-console-consumer --topic my-generic-record-value --bootstrap-server $khost --property schema.registry.url=http://127.0.0.1:8081
    @Test
    fun avroProducer(){
        val schemaPerson = Schema.Parser().parse("""
            {
              "name": "Person",
              "namespace": "myapp.infrastructure",
              "type": "record",
              "fields": [
                {"name": "firstName","type": "string"},
                {"name": "lastName","type": "string"},
                {"name": "age","type": "int"}
              ]
            }
        """.trimIndent())

        val genericRecordPerson = GenericRecordBuilder(schemaPerson).apply{
            set("firstName", "sam")
            set("lastName", "dedios")
            set("age", (0 until 10000).random())
        }.build()

        val topic = "my-generic-record-value"
        buildProducer().apply{
            send(ProducerRecord(topic, genericRecordPerson))
            flush()
            close()
        }
    }
}
