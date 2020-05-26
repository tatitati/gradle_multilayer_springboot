package infrastructure

import com.fasterxml.jackson.databind.ObjectMapper
import domain.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class LibraryEventProducer {

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<Int, String>

    @Autowired
    lateinit var objectMapper: ObjectMapper

    fun sendLibraryEventToDefaultTopic(libraryEvent: LibraryEvent) {
        val key = libraryEvent.libraryEventId
        val value = objectMapper.writeValueAsString(libraryEvent)
        val listenableFuture = kafkaTemplate.sendDefault(key, value)
        listenableFuture.addCallback({
            println(it!!.recordMetadata.toString())
        }, {
            println(it.toString())
        })
    }

    fun sendLibraryEventToCustomTopic(libraryEvent: LibraryEvent) {
        val key = libraryEvent.libraryEventId
        val value = objectMapper.writeValueAsString(libraryEvent)
        val listenableFuture = kafkaTemplate.send("library-events", key, value)
        listenableFuture.addCallback({
            println(it!!.recordMetadata.toString())
        }, {
            println(it.toString())
        })
    }

    fun sendLibraryEventToCustomTopicUsingProducerRecord(libraryEvent: LibraryEvent) {
        val key: Int = libraryEvent.libraryEventId
        val value: String = objectMapper.writeValueAsString(libraryEvent)

        val producerRecord: ProducerRecord<Int, String> = ProducerRecord("library-events", key, value)

        val listenableFuture = kafkaTemplate.send(producerRecord)
        listenableFuture.addCallback({
            println(it!!.recordMetadata.toString())
        }, {
            println(it.toString())
        })
    }
}
