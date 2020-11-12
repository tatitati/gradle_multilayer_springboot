package myapp.test.infrastructure.consumer

import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*


class ConsumerCommitToSpecificTopicsTests {
    val topics = listOf("topic_AA", "topic_BB", "topic_ERR")

    private fun printOffsets(message: String, consumer: KafkaConsumer<String, String>, topicPartition: TopicPartition) {
        val committed: Map<TopicPartition, OffsetAndMetadata> = consumer.committed(HashSet(Arrays.asList(topicPartition)))
        val offsetAndMetadata: OffsetAndMetadata? = committed[topicPartition]
        val position = consumer.position(topicPartition)
        System.out.printf("Offset info %s, Committed: %s, current position %s%n", message, if (offsetAndMetadata == null) null else offsetAndMetadata.offset(), position)
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
        consumer.subscribe(topics)
        return consumer
    }

    fun consume(consumer: KafkaConsumer<String, String>){
        val topicPartitionError = TopicPartition("topic_ERR", 0)
        val topicPartitionA = TopicPartition("topic_AA", 0)
        val topicPartitionB = TopicPartition("topic_BB", 0)

        val results = mutableListOf<ConsumerRecord<String, String>>()

        while(true){
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            records.forEach{ record ->
                val topic: String  = record.topic()
                val msg: String = record.value()
                val offset: Long = record.offset()

                println("\n\n$topic [$offset] -- $msg")
                results.add(record)


                val committed: Map<TopicPartition, OffsetAndMetadata> = consumer.committed(HashSet(Arrays.asList(topicPartitionA, topicPartitionB)))

                if(record.topic() == "topic_AA"){
                    println("\tcommiting")
                    var nextOffset = 1L
                    val offsetAndMetadataA: OffsetAndMetadata? = committed[topicPartitionA]
                    if(offsetAndMetadataA != null){
                        nextOffset = offsetAndMetadataA!!.offset() + 1
                    }
                    val newoffsetAndMetadataA = OffsetAndMetadata(nextOffset)
                    printOffsets("\tTopic AA: before commitAsync() call", consumer, topicPartitionA);
                    printOffsets("\tTopic BB: before commitAsync() call", consumer, topicPartitionB);
                    printOffsets("\tTopic ERR: before commitAsync() call", consumer, topicPartitionError);
                    consumer.commitSync(mapOf(topicPartitionA to newoffsetAndMetadataA))
                    printOffsets("\tTopic AA: after commitAsync() call", consumer, topicPartitionA);
                    printOffsets("\tTopic BB: after commitAsync() call", consumer, topicPartitionB);
                    printOffsets("\tTopic ERR: after commitAsync() call", consumer, topicPartitionError);
                }

                if(record.topic() == "topic_BB"){
                    println("\tcommiting")
                    val offsetAndMetadataB: OffsetAndMetadata? = committed[topicPartitionB]
                    var nextOffset = 1L
                    if(offsetAndMetadataB != null){
                        nextOffset = offsetAndMetadataB!!.offset() + 1
                    }
                    val newoffsetAndMetadataB = OffsetAndMetadata(nextOffset)
                    printOffsets("\tTopic AA: before commitAsync() call", consumer, topicPartitionA);
                    printOffsets("\tTopic BB: before commitAsync() call", consumer, topicPartitionB);
                    printOffsets("\tTopic ERR: before commitAsync() call", consumer, topicPartitionError);
                    consumer.commitSync(mapOf(topicPartitionB to newoffsetAndMetadataB))
                    printOffsets("\tTopic AA: after commitAsync() call", consumer, topicPartitionA);
                    printOffsets("\tTopic BB: after commitAsync() call", consumer, topicPartitionB);
                    printOffsets("\tTopic ERR: after commitAsync() call", consumer, topicPartitionError);
                }

                if(record.topic() == "topic_ERR"){
                    println("\tNot commiting")
                    printOffsets("\tTopic AA: before commitAsync() call", consumer, topicPartitionA);
                    printOffsets("\tTopic BB: before commitAsync() call", consumer, topicPartitionB);
                    printOffsets("\tTopic ERR: before commitAsync() call", consumer, topicPartitionError);
                    printOffsets("\tTopic AA: after commitAsync() call", consumer, topicPartitionA);
                    printOffsets("\tTopic BB: after commitAsync() call", consumer, topicPartitionB);
                    printOffsets("\tTopic ERR: after commitAsync() call", consumer, topicPartitionError);
                }
            }
        }
    }

    @Test
    fun run(){
        this.consume(buildConsumer())
    }
}
