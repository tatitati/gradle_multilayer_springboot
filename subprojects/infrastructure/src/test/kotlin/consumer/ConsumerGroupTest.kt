package myapp.test.infrastructure.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ConsumerGroupTest {

    fun buildConsumer(): KafkaConsumer<String, String>{
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-test")
        }

        val kafkaConsumer = KafkaConsumer<String, String>(
                properties
        )

        val topic = "consumer-group-topic"
        kafkaConsumer.subscribe(listOf(topic))
        return kafkaConsumer
    }

    @Test
    fun experiment(){
        val kafkaConsumer = buildConsumer()

        var receivedMessages = 0
        var limitReceived = 10
        while (true) {
            val records: ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.ofSeconds(2))
            records.iterator().forEach {
                receivedMessages++
                limitReceived--

                println("Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())

                if (limitReceived < 0) return
            }
        }
    }

    //    Result description:
    //    ===================
    //    The consumer read an specific amount of msgs from a topic
    //
    //
    //    Output:
    //    =======
    //    Partition: 0, Offset: 0, Key: null, Value: one
    //    Partition: 0, Offset: 1, Key: null, Value: two
    //    Partition: 0, Offset: 2, Key: null, Value: three
    //    Partition: 0, Offset: 3, Key: null, Value: four
    //    Partition: 0, Offset: 4, Key: null, Value: five
    //    Partition: 0, Offset: 5, Key: null, Value: six
    //    Partition: 0, Offset: 6, Key: null, Value: seven
    //    Partition: 0, Offset: 7, Key: null, Value: eight
    //    Partition: 0, Offset: 8, Key: null, Value: nine
    //    Partition: 0, Offset: 9, Key: null, Value: ten
    //    Partition: 0, Offset: 10, Key: null, Value: eleven
}
