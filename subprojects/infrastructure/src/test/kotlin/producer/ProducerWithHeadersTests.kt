package myapp.test.infrastructure.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.util.*

class ProducerWithHeadersTests {

    @Test
    fun run() {
        val topicInput = "kstream_inputXXX"
        val properties = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        val producer = KafkaProducer<String, String>(properties)

        for (i in 1..10) {
            val futureResult = producer.send(
                    ProducerRecord(
                            topicInput,
                            null,
                            null,
                            null,
                            "this is the value $i",
                            listOf(
                                    RecordHeader("error", "something".toByteArray())))
            )
            
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }
}
