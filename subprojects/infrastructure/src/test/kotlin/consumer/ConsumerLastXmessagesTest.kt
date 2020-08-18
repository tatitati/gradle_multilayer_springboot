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
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumergroup-ConsumerLastXmessagesTest")
        }

        return KafkaConsumer<String, String>(
                properties
        )
    }

    @Test
    fun experiment() {
        val consumer = buildConsumer()
        // params for seek and assign
        val partition = 0
        val getLastX = 2L

        val partitionToReadFrom: TopicPartition = TopicPartition("topic-ConsumerLastXmessagesTest", partition)

        consumer.apply{
            assign(listOf(partitionToReadFrom))
            seekToEnd(listOf(partitionToReadFrom))
        }

        val curentOffset = consumer.position(partitionToReadFrom)
        println("\n\n=========> starting to fetch batch from position: " + curentOffset)
        if (curentOffset <= getLastX) {
            consumer.seek(partitionToReadFrom, curentOffset)
        } else {
            consumer.seek(partitionToReadFrom, curentOffset - getLastX)
        }


        var limitReceived = getLastX
        while (true) {
            println("\n\nFetching batch....\n\n")
            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(2))
            println("==========> Received a batch with recods amount: " + batchOfRecords.count())

            // process batch
            batchOfRecords.iterator().forEach {
                limitReceived--

                println("\nBatch record ===> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())

                consumer.commitSync()
            }

            if (limitReceived <= 0) break

            Thread.sleep(2000)
        }
    }
}
