package myapp.test.infrastructure.schemaregistry

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
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
