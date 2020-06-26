package myapp.test.infrastructure.schemaregistry

import io.confluent.kafka.serializers.KafkaAvroSerializer
import myapp.infrastructure.Person
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class GenericRecordTest {
    fun buildProducer(): KafkaProducer<String, GenericRecord> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaAvroSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer<String, GenericRecord>(
                properties
        )
    }

    @Test
    fun testBatching(){
        val schemaPerson = Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Person",
              "namespace": "myapp.infrastructure",
              "fields": [
                {
                  "name": "firstName",
                  "type": "string"
                },
                {
                  "name": "lastName",
                  "type": "string"
                },
                {
                  "name": "age",
                  "type": "int"
                }
              ]
            }
        """.trimIndent())

        val genericRecordPerson = GenericRecordBuilder(schemaPerson).apply{
            set("firstName", "samuel")
            set("lastName", "ruiz")
            set("age", 4)
        }.build()


        val producer: KafkaProducer<String, GenericRecord> = buildProducer()

        val topic = "topic-generic-record-avro"
        producer.send(
                ProducerRecord(topic, genericRecordPerson)
        )

    }
}
