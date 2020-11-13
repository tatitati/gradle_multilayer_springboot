package myapp.test.infrastructure.consumer

import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*


class ConsumerCommitToSpecificTopicsTests {
    private fun printOffsets(message: String, consumer: KafkaConsumer<String, String>, topicPartition: TopicPartition) {
        val committed: Map<TopicPartition, OffsetAndMetadata> = consumer.committed(hashSetOf(topicPartition))
        val commitedOffsetAndMetadata: OffsetAndMetadata? = committed[topicPartition]
        println("\t\t${message}, offset: ${if (commitedOffsetAndMetadata == null) "no-metadata" else commitedOffsetAndMetadata.offset()}")
    }

    val topicA = "topic_A1"
    val topicE = "topic_E1"

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "aaa")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

        val consumer = KafkaConsumer<String, String>(properties)
        consumer.subscribe(listOf(topicA, topicE))
        return consumer
    }

    fun consume(consumer: KafkaConsumer<String, String>){
        var partitions: MutableSet<TopicPartition> = mutableSetOf()

        while(true){
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            if(partitions.isEmpty()){
                partitions = consumer.assignment()
            }

            records.forEach{ record ->
                val recordTopic: String  = record.topic()
                val recordPartition: Int = record.partition()
                val recordOffset: Long = record.offset()

                println("\n\n$recordTopic [$recordPartition($recordOffset)] -- ${record.value()}")

                if(recordTopic == topicA){
                    val topicPartition = TopicPartition(recordTopic, recordPartition)

                    printOffsets("\tBEFORE", consumer, topicPartition);
                    consumer.commitSync(mapOf(topicPartition to OffsetAndMetadata(recordOffset + 1)))
                    printOffsets("\tAFTER", consumer, topicPartition);
                }
            }
        }
    }

    @Test
    fun run(){
        this.consume(buildConsumer())
    }
}
