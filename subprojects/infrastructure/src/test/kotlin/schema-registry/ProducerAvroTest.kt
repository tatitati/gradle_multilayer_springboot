package myapp.test.infrastructure.`schema-registry`

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import java.util.*

class ProducerAvroTest {
    fun buildProducer(): KafkaProducer<String, String> {
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            // params for batching
            put(ProducerConfig.LINGER_MS_CONFIG, "2000")
            put(ProducerConfig.BATCH_SIZE_CONFIG, (32*1024).toString()) //32KB batch size
        }

        return KafkaProducer<String, String>(
                properties
        )
    }

    @Test
    fun testBatching(){
        val producer = buildProducer()

        val topic = "batching-producer"
        val msgs = arrayOf("ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVE", "EIGHT", "NINE", "TEN")

        msgs.forEach{
            Thread.sleep(500)
            println("sending item: " + it)
            producer.send(
                    ProducerRecord(topic, it)
            )
        }
    }
}
