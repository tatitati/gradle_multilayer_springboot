package myapp.test.infrastructure.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.util.*

class ProducerIdempotentTest {

    fun buildProducer(): KafkaProducer<String, String>{
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            // params for safe producer
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE.toString())
            put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
        }

        return KafkaProducer<String, String>(
                properties
        )
    }

    @Test
    fun testBatching(){
        val producer = buildProducer()
        val topic = "safe-producer"
        val msgs = arrayOf("ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVE", "EIGHT", "NINE", "TEN")

        msgs.forEach{
            producer.send(
                    ProducerRecord(topic, it))
        }

        producer.flush()
        producer.close()
    }
}
