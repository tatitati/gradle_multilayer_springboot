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

class ProducerUsingJsonSchemaDraft7Test {
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

    // you can test the producer with the CLI:
    // kafka-avro-console-consumer --topic my-generic-record-value --bootstrap-server $khost --property schema.registry.url=http://127.0.0.1:8081
    @Test
    fun createSchemaRecord(){
        val schema = """"""" + "\$" + "schema" + """""""
        val id = """"""" + "\$" + "id" + """""""

        val schemaPersonUsingDraft7 = Schema.Parser().parse("""
            {
                $schema: "http://json-schema.org/draft-07/schema#",
                $id: "anewid",
                "title": "The Root Schema",
                "name": "asdfasdf",
                "subject": "newcategory",
                "schemaType": "JSON",
                "type": "record",
                "fields": [
                        {"name": "aaa", "type": "string","format": "date-time"}
                ], 
                "required": ["aaa"]
            }
        """.trimIndent())

        val genericRecordPerson = GenericRecordBuilder(schemaPersonUsingDraft7).apply{
            set("aaa", "2018-11-13T20:20:39+00:00")
        }.build()

        val producer: KafkaProducer<String, GenericRecord> = buildProducer()

        // the topic is generated if doesnt exist
        // the schema is generated if doesnt exist, is named: my-generic-record-value-value
        // (it behaves in the same way that the CLI avro-producer
        val topic = "my-topic-with-schema-record"
        producer.send(
                ProducerRecord(topic, genericRecordPerson)
        )

        producer.flush()
        producer.close()
    }
}
