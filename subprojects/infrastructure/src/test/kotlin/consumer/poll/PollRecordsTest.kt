package myapp.test.infrastructure.consumer.poll

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.LocalDateTime
import java.util.*

class PollRecordsTest {

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-poll")
        }

        val consumer = KafkaConsumer<String, String>(
                properties
        )

        consumer.subscribe(listOf("Consumer-PollRecordTest"))
        return consumer
    }

    @Test
    fun experiment(){
        val consumer = buildConsumer()

        while (true) {
            val current = LocalDateTime.now()
            println("\n\n\n====>Time: $current")

            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(20))
            val current2 = LocalDateTime.now()
            println("====> Poll finished. Time is: $current2")
            batchOfRecords.iterator().forEach {
                println("====> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
            }
        }
    }

    // Experiment 1: We don't send any message to the topic, we can see that the application block 20 seconds (the 20 seconds used as timeout for poll)
    // ==================
    //    ====>Time: 2020-09-09T22:59:48.217
    //    ====> Poll finished. Time is: 2020-09-09T23:00:08.219
    //    ====> Received a batch with recods amount: 0
    //
    //    ....AFTER 20 SECONDS...
    //
    //    ====>Time: 2020-09-09T23:00:08.219
    //    ====> Poll finished. Time is: 2020-09-09T23:00:28.222
    //    ====> Received a batch with recods amount: 0


    // Experiment 2: We send data to the topic. We send that poll doesn't wait for 20 seconds, as soon as it receives some data, poll returns:
    // ===================
    //    ====>Time: 2020-09-09T23:04:01.745
    //    ====> Poll finished. Time is: 2020-09-09T23:04:10.090
    //    ====> Received a batch with recods amount: 1
    //    ====> Partition: 0, Offset: 5, Key: null, Value: six
    //
    //    ....AFTER 3 SECONDS...
    //
    //    ====>Time: 2020-09-09T23:04:10.090
    //    ====> Poll finished. Time is: 2020-09-09T23:04:13.844
    //    ====> Received a batch with recods amount: 1
    //    ====> Partition: 0, Offset: 6, Key: null, Value: seven
}
