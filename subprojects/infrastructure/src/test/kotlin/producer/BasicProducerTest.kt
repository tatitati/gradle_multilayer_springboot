package myapp.test.infrastructure.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.util.*

class BasicProducerTest {
    fun buildProducer(): KafkaProducer<String, String> {
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        return KafkaProducer<String, String>(
                properties
        )
    }

    @Test
    fun basicProducer(){
        val producer = buildProducer()

        val topic = "basic-producer"

        producer.send(
                ProducerRecord(topic, "this is my msg")
        )

        producer.flush()
        producer.close()
    }
}