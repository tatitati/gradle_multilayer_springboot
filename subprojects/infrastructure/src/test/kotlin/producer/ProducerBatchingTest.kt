package myapp.test.infrastructure.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Test
import java.util.*

class ProducerBatchingTest {

    fun buildProducer(): KafkaProducer<String, String>{
        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            // params for batching
            put(ProducerConfig.LINGER_MS_CONFIG, "2000")
            put(ProducerConfig.BATCH_SIZE_CONFIG, (32*1024).toString()) //32KB batch size
        }

        return KafkaProducer<String, String>(
                properties
        )
    }

    @Test
    fun testBatching(){
        val producer = buildProducer()

        val msgs = arrayOf("ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE", "TEN")

        msgs.forEach{ msg ->
            Thread.sleep(500)
            println("sending item: " + msg)
            producer.send(
                    ProducerRecord("batching-producer", msg)
            )
        }

        producer.apply{
            flush()
            close()
        }
    }

    // Output description:
    // ===================
    // The producer wait 500ms, so in the Linger period (2000ms), it can stack 4 messages.
    // We can see that the producer sends messages in batches of 4:
    //
    //
    // OUPUT:
    // =====-
    //    Topic: batching-producer, Croup: mygroup, Partition: 0, offset: 8, key: , payload:  ONE
    //    Topic: batching-producer, Croup: mygroup, Partition: 0, offset: 9, key: , payload:  TWO
    //    Topic: batching-producer, Croup: mygroup, Partition: 0, offset: 10, key: , payload:  THREE
    //    Topic: batching-producer, Croup: mygroup, Partition: 0, offset: 11, key: , payload:  FOUR
    //    % Reached end of topic batching-producer [0] at offset 12
    //
    //    Topic: batching-producer, Croup: mygroup, Partition: 0, offset: 12, key: , payload:  FIVE
    //    Topic: batching-producer, Croup: mygroup, Partition: 0, offset: 13, key: , payload:  SIX
    //    Topic: batching-producer, Croup: mygroup, Partition: 0, offset: 14, key: , payload:  SEVE
    //    Topic: batching-producer, Croup: mygroup, Partition: 0, offset: 15, key: , payload:  EIGHT
}
