package myapp.infrastructure.kafkastream

import org.apache.kafka.streams.KafkaStreams

class RepositoryKStreams(
    private val kStreams: KafkaStreams
){
    fun start(){
        kStreams.start()
    }
}
