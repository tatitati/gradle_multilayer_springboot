package myapp.test.infrastructure.producer

import myapp.infrastructure.kafkastream.pojos.Person
import myapp.infrastructure.kafkastream.serdes.PersonSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import java.util.*
import javax.annotation.PostConstruct

class ProducerWithCustomSerializerTest {
    val topicInput = "topic-input-person"

    @Test
    fun run(){
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PersonSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerializer::class.java)
        }

        val producer = KafkaProducer<String, Person>(properties)

        for(i in 1..10) {
            val person = Person(
                    firstName = "firstname"+i,
                    lastName = "lastName"+i,
                    age = 86
            )
            val futureResult = producer.send(ProducerRecord(topicInput, person))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }
}
