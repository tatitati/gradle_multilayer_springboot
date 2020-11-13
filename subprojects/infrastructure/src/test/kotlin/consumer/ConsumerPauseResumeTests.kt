package myapp.test.infrastructure.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ConsumerPauseResumeTests {
    val topicInput = "topic_pause"

    fun buildConsumer(): KafkaConsumer<String, String> {
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "aaa")
//          put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true") // by default every 5 secs
//          put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // by default
        }

        val consumer = KafkaConsumer<String, String>(properties)
        consumer.subscribe(listOf(topicInput))
        return consumer
    }

    fun consume(consumer: KafkaConsumer<String, String>){
        while(true){
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            records.forEach{ record ->
                val topic: String  = record.topic()
                val msg: String = record.value()
                val offset: Long = record.offset()

                println("[$offset] $topic -- $msg")

                if(record.value() == "pause"){
                    val assignments = consumer.assignment()
                    consumer.pause(assignments)
                }

                // BE AWARE: this code is useless, once is stopped, none record will be consummed to check if is resume or not, is paused!
                // if(record.value() == "resume"){
                //     val assignments = consumer.assignment()
                //     consumer.resume(assignments)
                // }
            }
        }
    }

    @Test
    fun run(){
        this.consume(buildConsumer())
    }
}
