package myapp.test.infrastructure.consumer.OffsetCommit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.LocalDateTime
import java.util.*

class AutoCommitTest {

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-autocommitoffset")
            // automatic commit of offsets
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true") // by default every 5 secs
            put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "20000")
        }

        val consumer = KafkaConsumer<String, String>(
                properties
        )

        consumer.subscribe(listOf("Consumer_AutoCommitTest"))
        return consumer
    }

    @Test
    fun experiment(){
        val consumer = buildConsumer()

        while (true) {
            val current = LocalDateTime.now()
            println("\n\n\n======> Time: $current")

            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(60))
            val current2 = LocalDateTime.now()
            println("======> Poll completed. Time: $current2")
            println("======> Received a batch with recods amount: " + batchOfRecords.count())

            batchOfRecords.iterator().forEach {
                println("======> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
            }
        }
    }
}
