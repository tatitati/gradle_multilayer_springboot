package myapp.test.infrastructure.consumer.OffsetReset

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ConsumerSeekAndAssignTest {

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        }

        return KafkaConsumer<String, String>(
                properties
        )
    }

    @Test
    fun experiment(){
        val consumer = buildConsumer()
        val topic = "my-seek-assign-topic"
        // params for seek and assign
        val partition = 0
        val offset = 15L
        val partitionToReadFrom: TopicPartition = TopicPartition(topic, partition)

        consumer.assign(listOf(partitionToReadFrom))
        consumer.seek(partitionToReadFrom, offset)

        var receivedMessages = 0
        var limitReceived = 15
        while (true) {
            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(2))
            println("Received a batch with recods amount: " + batchOfRecords.count())

            // process batch
            batchOfRecords.iterator().forEach {
                receivedMessages++
                limitReceived--

                println("Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
            }

            if (limitReceived < 0) break
        }
    }

    //    Result description:
    //    ===================
    //    We specified an offset of 15L. So kafka start at that offset to read
    //
    //
    //    Output:
    //    =======
    //    Partition: 0, Offset: 15, Key: null, Value: sixteen
    //    Partition: 0, Offset: 16, Key: null, Value: seventeen
    //    Partition: 0, Offset: 17, Key: null, Value: eighteen
    //    Partition: 0, Offset: 18, Key: null, Value: nineteen
    //    Partition: 0, Offset: 19, Key: null, Value: twenty
    //    Partition: 0, Offset: 20, Key: null, Value: twenty one
    //    Partition: 0, Offset: 21, Key: null, Value: twenty two
    //    Partition: 0, Offset: 22, Key: null, Value: twenty three
}
