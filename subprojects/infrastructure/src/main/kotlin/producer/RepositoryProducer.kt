package myapp.infrastructure.producer

import myapp.domain.Book
import myapp.infrastructure.MapperBook
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration
import java.util.concurrent.Future

class RepositoryProducer(
        private val kProducer: KafkaProducer<String, String>,
        private val mapper: MapperBook
){
    fun sendToTopic(book: Book, topic: String): Future<RecordMetadata> {
        val record: ProducerRecord<String, String> = ProducerRecord(
                topic,
                mapper.toJson(book))

        return kProducer.send(record)
    }
}
