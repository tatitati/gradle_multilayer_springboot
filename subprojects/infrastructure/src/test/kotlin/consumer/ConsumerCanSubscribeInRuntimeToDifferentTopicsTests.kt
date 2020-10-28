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

class ConsumerCanSubscribeInRuntimeToDifferentTopicsTests {
    val topicError1 = "error55"
    val topicError2 = "error66"


    // FIXTURES
    fun fixtures(topic: String){
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        val producer = KafkaProducer<String, String>(properties)

        for (i in 1..20) {
            val futureResult = producer.send(ProducerRecord(
                    topic,
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
            put(ConsumerConfig.GROUP_ID_CONFIG, "asdfasdf")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5")
        }

        return KafkaConsumer<String, String>(properties)
    }

    fun consume(consumer: KafkaConsumer<String, String>, max: Int = 5){
        val results = mutableListOf<ConsumerRecord<String, String>>()
        while(results.size < max){
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
        val consumer = buildConsumer()

        this.fixtures(topicError1)
        consumer.subscribe(listOf(topicError1))
        this.consume(consumer, max = 10)

        println("subscribing to different topics...")
        Thread.sleep(5000)

        consumer.subscribe(listOf(topicError2, topicError1))
        this.fixtures(topicError2)
        this.fixtures(topicError1)
        this.consume(consumer, max = 1000)
    }
}
