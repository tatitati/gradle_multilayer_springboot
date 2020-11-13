package myapp.test.infrastructure.producer

import myapp.test.domain.Faker
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

        return KafkaProducer(properties)
    }

    @Test
    fun basicProducer(){
        buildProducer().apply{
            send(ProducerRecord("topic-BasicProducerTest", "this is my msg"))
            flush()
            close()
        }
    }

    @Test
    fun fakerCanPopulateTopic(){
        Faker.sentEventsToTopic(
                "topic-BasicProducerTest",
                Faker.anyListFromBuilder({Faker.anyWord()}, 10)
        )
    }
}
