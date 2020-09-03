package myapp.test.infrastructure.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.junit.jupiter.api.Test
import java.util.*

class ProducerIdempotentWithTransactionsTest {

    fun buildProducer(): KafkaProducer<String, String>{
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            // params for safe producer
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_idempotent_producer");

        }

        return KafkaProducer<String, String>(
                properties
        )
    }

    @Test
    fun testIdempotentProducer(){
        val msgs = arrayOf("ONE", "TWO", "THREE", "FOUR")

        val producer = buildProducer()

        producer.initTransactions()
        try {
            msgs.forEach{ msg ->
                producer.beginTransaction()
                producer.send(ProducerRecord<String, String>("topic-ProducerIdempotentTest", msg))
                producer.commitTransaction();
            }

        } catch (e: KafkaException){
            print ("exception")
            producer.abortTransaction();
        }

        // NOW IN ANOTHER TRANSACTION WE TRY again the same, but these messages won't be sent as they are duplicates :)
        producer.initTransactions()
        try {
            msgs.forEach{ msg ->
                producer.beginTransaction()
                producer.send(ProducerRecord<String, String>("topic-ProducerIdempotentTest", msg))
                producer.commitTransaction();
            }

        } catch (e: KafkaException){
            print ("exception")
            producer.abortTransaction();
        }

        producer.apply{
            flush()
            close()
        }
    }
}
