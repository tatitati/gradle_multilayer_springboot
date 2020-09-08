package myapp.test.infrastructure.consumer.poll

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class PollRecordsTest {

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-batch-size")
            // Limit batches size in consumer
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2")
        }

        val consumer = KafkaConsumer<String, String>(
                properties
        )

        consumer.subscribe(listOf("consumer-batch-size-topic"))
        return consumer
    }

    @Test
    fun experiment(){
        val consumer = buildConsumer()

        while (true) {
            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(10000))
            println("\n\n==========> Received a batch with recods amount: " + batchOfRecords.count())
            batchOfRecords.iterator().forEach {
                println("=========> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
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
    //    10 seconds....
    //
    //    ==========> Received a batch with recods amount: 2
    //    =========> Partition: 0, Offset: 4, Key: null, Value: five
    //    =========> Partition: 0, Offset: 5, Key: null, Value: six
}
