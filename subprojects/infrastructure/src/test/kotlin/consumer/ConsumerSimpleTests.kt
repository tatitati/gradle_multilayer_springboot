package myapp.test.infrastructure.consumer

import myapp.test.domain.Faker
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ConsumerSimpleTests {
    val topicInput = "topic_inputSimple"


    // FIXTURES
    fun fixtures(){
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        val producer = KafkaProducer<String, String>(properties)

        for (i in 1..20) {
            val futureResult = producer.send(ProducerRecord(
                    topicInput,
                    "${Faker.anyWord()}"
            ))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "aaa")
//          put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true") // by default every 5 secs
//          put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // by default
        }

        val consumer = KafkaConsumer<String, String>(properties)
        consumer.subscribe(listOf(topicInput))
        return consumer
    }

    fun consume(consumer: KafkaConsumer<String, String>){
        val results = mutableListOf<ConsumerRecord<String, String>>()

        while(true){
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            records.forEach{ record ->
                val topic: String  = record.topic()
                val msg: String = record.value()
                val offset: Long = record.offset()

                println("[$offset] $topic -- $msg")
                results.add(record)
            }
        }
    }

    @Test
    fun run(){
        this.fixtures()
        this.consume(buildConsumer())

    }
}
