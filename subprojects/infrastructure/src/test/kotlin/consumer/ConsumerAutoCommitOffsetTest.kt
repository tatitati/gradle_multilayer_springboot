package myapp.test.infrastructure.consumer

import org.apache.commons.logging.impl.Log4JLogger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.logging.log4j.LogBuilder
import org.apache.logging.log4j.Logger
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import java.time.Duration
import java.util.*

class ConsumerAutoCommitOffsetTest {

    fun buildConsumer(): KafkaConsumer<String, String>{
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-test")
            // automatic commit of offsets
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
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

        while (true) {
            val batchOfRecords: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(20))
            println("Received a batch with recods amount: " + batchOfRecords.count())

            batchOfRecords.iterator().forEach {
                println("=========> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
                consumer.commitSync()
            }
        }
    }
}
