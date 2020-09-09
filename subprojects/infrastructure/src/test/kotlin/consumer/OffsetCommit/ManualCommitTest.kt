package myapp.test.infrastructure.consumer.OffsetCommit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ManualCommitTest {

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-manualcommitoffset")
            // manual commit of offsets
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        }

        val consumer = KafkaConsumer<String, String>(
                properties
        )

        consumer.subscribe(listOf("Consumer_ManualCommitTest"))
        return consumer
    }

    @Test
    fun experiment(){
        val consumer = buildConsumer()

        while (true) {
            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(20))
            println("\n\n\nReceived a batch with records amount: " + batchOfRecords.count())

            batchOfRecords.iterator().forEach {
                println("=========> Record:")
                println("\t=========> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
                Thread.sleep(5_000) // 5 secs
            }

            // commitAsync commit the offset for all the records fetched. It doesnt make sense to put this
            // inside the loop, because it doesn't commit record by record, byt the whole batch
            consumer.commitSync()
            println("\t=========> offset commited")
        }
    }
}
