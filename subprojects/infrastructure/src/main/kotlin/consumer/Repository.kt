package myapp.infrastructure.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

class Repository(
        private val kconsumer: KafkaConsumer<String, String>
){
    fun consumeFromTopic(topic: String) {
        kconsumer.subscribe(listOf(topic))

        while (true) {
            val records: ConsumerRecords<String, String> = kconsumer.poll(Duration.ofSeconds(1))
            records.iterator().forEach {
                println(it)
            }
        }
    }
}
