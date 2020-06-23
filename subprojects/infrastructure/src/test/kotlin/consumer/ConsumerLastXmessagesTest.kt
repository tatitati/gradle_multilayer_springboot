package myapp.test.infrastructure.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ConsumerLastXmessagesTest {
    fun buildConsumer(): KafkaConsumer<String, String>{
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-lastx-from-topic")
        }

        return KafkaConsumer<String, String>(
                properties
        )
    }

    @Test
    fun experiment() {
        val consumer = buildConsumer()
        val topic = "consumer-lastx-from-topic"
        // params for seek and assign
        val partition = 0
        val getLastX = 2L

        val partitionToReadFrom: TopicPartition = TopicPartition(topic, partition)


        consumer.assign(listOf(partitionToReadFrom))
        consumer.seekToEnd(listOf(partitionToReadFrom))
        val currentPosition = consumer.position(partitionToReadFrom)
        println("\n\n=========> current position: " + currentPosition)
        if (currentPosition <= getLastX) {
            consumer.seek(partitionToReadFrom, currentPosition)
        } else {
            consumer.seek(partitionToReadFrom, currentPosition - getLastX)
        }


        var limitReceived = getLastX
        while (true) {
            println("\n\n=============================\n\n")
            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(2))
            println("\n\n==========> Received a batch with recods amount: " + batchOfRecords.count())

            // process batch
            batchOfRecords.iterator().forEach {
                limitReceived--

                println("\n\n==========> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())

                consumer.commitSync()
            }

            if (limitReceived <= 0) break
        }
    }
}
