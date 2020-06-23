package myapp.infrastructure.consumer

import myapp.infrastructure.MapperBook
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

class RepositoryConsumer(
        private val kconsumer: KafkaConsumer<String, String>,
        private val mapper: MapperBook
){
    fun consumeFromTopic(topic: String) {
        kconsumer.subscribe(listOf(topic))

        while (true) {
            val batchOfRecords: ConsumerRecords<String, String> = kconsumer.poll(Duration.ofSeconds(1))
            // process batch
            batchOfRecords.iterator().forEach {
                println(it)
            }
        }
    }
}
