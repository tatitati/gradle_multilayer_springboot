package myapp.test.infrastructure.reingest

import myapp.test.domain.Faker
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*
import java.util.concurrent.Future

class ReingestorTests {
    val topicError = "topic.error"
    val topicInput = "topic.input"
    var consumedErrorMsgs = mutableListOf<String>()


    // ERROR FIXTURES
    fun fixturesErrorTopic(){
        val producer: KafkaProducer<String, String> = this.buildProducer()

        for (i in 1..10) {
            val futureResult: Future<RecordMetadata> = producer.send(ProducerRecord(topicError, "${Faker.anyWord()}"))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }

    fun buildProducer(): KafkaProducer<String, String> {
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        return KafkaProducer<String, String>(properties)
    }

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, Faker.anyWord())
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5")
        }

        val consumer = KafkaConsumer<String, String>(properties)

        return consumer
    }

    fun consumerAndStore(consumer: KafkaConsumer<String, String>, amount: Int){
        var counter = 0
        while(counter < amount){
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            records.forEach{ record ->
                val topic: String  = record.topic()
                val msg: String = record.value()
                val offset: Long = record.offset()

                println("[$offset] $topic -- $msg")
                consumedErrorMsgs.add(record.value())
                counter++
            }
        }
    }

    fun reingest(){
        val producer = this.buildProducer()

        for (consumedInerrorMsg in consumedErrorMsgs) {
            val futureResult: Future<RecordMetadata> = producer.send(ProducerRecord(topicInput, consumedInerrorMsg))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }

    @Test
    fun run(){

//        this.fixturesErrorTopic()

        println("consuming and storing...")
        val consumer = buildConsumer()
        consumer.subscribe(listOf(topicError))
        this.consumerAndStore(consumer, 10)


        println("reingesting......")
        this.reingest()


        Thread.sleep(30000)
    }
}
