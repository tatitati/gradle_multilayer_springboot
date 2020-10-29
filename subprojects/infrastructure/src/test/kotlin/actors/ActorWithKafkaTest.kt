package myapp.test.infrastructure.actors

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelIterator
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

open class Message(){}


class MessageStop(): Message() {
}

class MessageStart(): Message() {
}

class MessageHeartbeat(): Message() {
}



class ActorWithKafkaTest {

    @Test
    fun `I can put a kafka consumer and control it with actors`(){
        val properties = Properties().apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "fgg")
        }

        val consumer = KafkaConsumer<String, String>(properties)
        consumer.subscribe(listOf("actortopic"))

        fun CoroutineScope.consumerActor() = actor<Message> {
            while(true) {
                val message: Message? = channel.poll()
                when(message) {
                    is MessageStop -> {
                        println("STOP MESSAGE RECEIVED")
                        consumer.close()
                    }
                    else -> {
                        val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
                        println("POLLING......")
                        for(record in records){
                            println(record.value())
                        }
                    }
                }
            }
        }

        runBlocking<Unit> {
            val consumerActor = consumerActor()
            consumerActor.send(MessageStart())
            val deferred = CompletableDeferred<Int>()
            Thread.sleep(10000)
            consumerActor.send(MessageStop())
            println(deferred.await())
            consumerActor.close()
        }



    }
}
