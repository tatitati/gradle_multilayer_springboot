package myapp.infrastructure

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.stereotype.Repository

class RepositoryConsumer(
        private val kconsumer: KafkaConsumer<String, String>
){

}
