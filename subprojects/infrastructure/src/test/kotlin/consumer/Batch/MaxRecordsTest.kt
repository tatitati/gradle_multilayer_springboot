package myapp.test.infrastructure.consumer.Batch

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class MaxRecordsTest {

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

        consumer.subscribe(listOf("Consumer-MaxRecordsTest"))
        return consumer
    }

    @Test
    fun experiment(){
        val consumer = buildConsumer()

        while (true) {
            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(20))
            println("\n\n\n====> Received a batch with recods amount: " + batchOfRecords.count())
            batchOfRecords.iterator().forEach {
                println("====> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
            }
        }
    }

    // Experiment 1: we send one message. The batch configuration allow to receive, as a maximum, two records, but if we send only one, is fine
    // =================================
    //    ====> Received a batch with recods amount: 1
    //    ====> Partition: 0, Offset: 0, Key: null, Value: my message

    // Experiment 2: we send 5 messages, but the batch configuration only allow to receive maximum two records
    // ================================
    //    ====> Received a batch with recods amount: 2
    //    ====> Partition: 0, Offset: 15, Key: null, Value: df
    //    ====> Partition: 0, Offset: 16, Key: null, Value: asdf
    //
    //
    //
    //    ====> Received a batch with recods amount: 2
    //    ====> Partition: 0, Offset: 17, Key: null, Value: asdf
    //    ====> Partition: 0, Offset: 18, Key: null, Value: asdf
    //
    //
    //
    //    ====> Received a batch with recods amount: 1
    //    ====> Partition: 0, Offset: 19, Key: null, Value: asdf

}
