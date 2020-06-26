package myapp.test.infrastructure.schemaregistry

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class ProducerAvroTest {
    fun buildProducer(): KafkaProducer<String, String> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaAvroSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer<String, String>(
                properties
        )
    }

    @Test
    fun testBatching(){
        val jsonSchemaPerson = """
            {
              "type": "record",
              "name": "Person",
              "namespace": "com.ippontech.kafkatutorials",
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
                  "name": "birthDate",
                  "type": "long"
                }
              ]
            }
        """.trimIndent()

//        val schema = Schema.Parser().parse(File("src/main/resources/person.avsc"))
        val schemaPerson = Schema.Parser().parse(jsonSchemaPerson)


        data class Person(
                val firstName: String,
                val lastName: String,
                val age: Int
        )

        val avroPerson = GenericRecordBuilder(schemaPerson)
        avroPerson.set("firstName", "samuel")
        avroPerson.set("lastName", "ruiz")
        avroPerson.set("age", 4)
        val recordPerson = avroPerson.build()


        val producer: KafkaProducer<String, Costumer> = buildProducer()

        val topic = "customer-avro"
        val msgs = arrayOf("ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVE", "EIGHT", "NINE", "TEN")

        msgs.forEach{
            producer.send(
                    ProducerRecord(topic, it)
            )
        }
    }
}
