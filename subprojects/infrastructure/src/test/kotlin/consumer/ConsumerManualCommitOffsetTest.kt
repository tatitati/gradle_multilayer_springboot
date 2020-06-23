package myapp.test.infrastructure.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ConsumerManualCommitOffsetTest {

    fun buildConsumer(): KafkaConsumer<String, String>{
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-test")
            // manual commit of offsets
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        }

        val consumer = KafkaConsumer<String, String>(
                properties
        )

        val topic = "consumer-group-topic"
        consumer.subscribe(listOf(topic))
        return consumer
    }

    @Test
    fun experiment(){
        val consumer = buildConsumer()

        var receivedMessages = 0
        var limitReceived = 10
        while (true) {
            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(2))
            println("Received a batch with recods amount: " + batchOfRecords.count())

            // process batch
            batchOfRecords.iterator().forEach {
                receivedMessages++
                limitReceived--

                println("=========> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
                consumer.commitSync()

                if (limitReceived < 0) return
            }
        }
    }
}
