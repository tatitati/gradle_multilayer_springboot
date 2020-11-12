package myapp.test.infrastructure.consumer

import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*


class ConsumerCommitToSpecificTopicsTests {
    private fun printOffsets(message: String, consumer: KafkaConsumer<String, String>, topicPartition: TopicPartition) {
        val committed: Map<TopicPartition, OffsetAndMetadata> = consumer.committed(HashSet(Arrays.asList(topicPartition)))
        val offsetAndMetadata: OffsetAndMetadata? = committed[topicPartition]
        println("\t\t${message}, offset: ${if (offsetAndMetadata == null) null else offsetAndMetadata.offset()}")
    }

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
        consumer.subscribe(listOf("topic__AA", "topic__ER"))
        return consumer
    }

    fun consume(consumer: KafkaConsumer<String, String>){
        var partitions: MutableSet<TopicPartition> = consumer.assignment()

        while(true){
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            if(partitions.isEmpty()){
                partitions = consumer.assignment()
            }

            partitions.forEach{p ->
                // println("PARTITIONS: ${p.topic()} / ${p.partition()}")
                // PARTITIONS: topic__AA / 1
                // PARTITIONS: topic__ER / 0
                // PARTITIONS: topic__ER / 1
                // PARTITIONS: topic__AA / 0
            }

            records.forEach{ record ->
                val recordTopic: String  = record.topic()
                val recordPartition: Int = record.partition()
                val recordOffset: Long = record.offset()

                println("\n\n$recordTopic [$recordPartition($recordOffset)] -- ${record.value()}")

                if(recordTopic == "topic__AA"){
                    val topicPartitionA = TopicPartition(recordTopic, recordPartition)

                    // CALCULATE NEXT OFFSET TO COMMIT
                    val allOffsetCommitedToPartitions: Map<TopicPartition, OffsetAndMetadata> = consumer.committed(hashSetOf(topicPartitionA))
                    val lastCommitedOffsetInPartition: OffsetAndMetadata? = allOffsetCommitedToPartitions[topicPartitionA]
                    var nextCommitOffsetInpartition = 1L
                    if(lastCommitedOffsetInPartition != null){
                        nextCommitOffsetInpartition = lastCommitedOffsetInPartition!!.offset() + 1
                    }

                    printOffsets("\tBEFORE", consumer, topicPartitionA);
                    consumer.commitSync(mapOf(topicPartitionA to OffsetAndMetadata(nextCommitOffsetInpartition)))
                    printOffsets("\tAFTER", consumer, topicPartitionA);
                }


                if(recordTopic == "topic__ER"){
                    val topicPartitionError = TopicPartition(recordTopic, recordPartition)
                    printOffsets("\tCURRENT", consumer, topicPartitionError);
                }
            }
        }
    }

    @Test
    fun run(){
        this.consume(buildConsumer())
    }
}
