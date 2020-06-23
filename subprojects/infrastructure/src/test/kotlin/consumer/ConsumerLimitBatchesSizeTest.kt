package myapp.test.infrastructure.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ConsumerLimitBatchesSizeTest {

    fun buildConsumer(): KafkaConsumer<String, String>{
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            // Limit batches size in consumer
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-batch-size")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2")
        }

        val kafkaConsumer = KafkaConsumer<String, String>(
                properties
        )

        val topic = "consumer-batch-size-topic"
        kafkaConsumer.subscribe(listOf(topic))
        return kafkaConsumer
    }

    @Test
    fun experiment(){
        val kafkaConsumer = buildConsumer()

        var receivedMessages = 0
        var limitReceived = 10
        while (true) {
            val batchOfRecords: ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.ofSeconds(2))
            println("\n\n==========> Received a batch with recods amount: " + batchOfRecords.count())
            Thread.sleep(1000)
            // process batch
            batchOfRecords.iterator().forEach {
                receivedMessages++
                limitReceived--

                println("=========> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())

                if (limitReceived < 0) return
            }
        }
    }

    // DESCRIPTION OUTPUT:
    // ==================
    // Our consumer process messages in batches of two
    //
    //
    // OUPUT:
    // =====
    //    ==========> Received a batch with recods amount: 2
    //    =========> Partition: 0, Offset: 2, Key: null, Value: three
    //    =========> Partition: 0, Offset: 3, Key: null, Value: four
    //
    //
    //    ==========> Received a batch with recods amount: 2
    //    =========> Partition: 0, Offset: 4, Key: null, Value: five
    //    =========> Partition: 0, Offset: 5, Key: null, Value: six
}
