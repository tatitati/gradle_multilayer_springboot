package myapp.test.infrastructure

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

class ProducerBatchingTest {

    fun buildProducer(): KafkaProducer<String, String>{
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "")
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "")
        }

        return KafkaProducer<String, String>(
                properties
        )
    }
}
