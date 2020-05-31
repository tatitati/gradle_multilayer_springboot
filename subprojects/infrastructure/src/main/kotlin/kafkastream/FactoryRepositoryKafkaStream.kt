package myapp.infrastructure.kafkastream

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*

@Configuration
class FactoryRepositoryKafkaStream(
        @Value ("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
        @Value ("\${spring.kafka.kstreams.input-topic}") private val inputTopic: String,
        @Value ("\${spring.kafka.kstreams.output-topic}") private val outputTopic: String,
        @Value ("\${spring.kafka.kstreams.input-topic2}") private val inputTopic2: String,
        @Value ("\${spring.kafka.kstreams.output-topic2}") private val outputTopic2: String
) {
//    @Bean
//    fun singleKafkaStream(): RepositoryKStreams {
//        val properties = Properties()
//        properties.apply{
//            put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application")
//            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
//            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//        }
//
//        val streamsBuilder = StreamsBuilder()
//
//        val inputStream: KStream<String, String> = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
//
//        val processedStream: KStream<String, String> = inputStream
//                .mapValues { textLine ->
//                    textLine.toLowerCase()
//                }
//                .flatMapValues { loweredCase ->
//                    loweredCase.split(" ")
//                }
//                .mapValues { splitText ->
//                    println(splitText)
//                    splitText
//                }
//
//        processedStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))
//
//        val topology: Topology = streamsBuilder.build()
//        val streams: KafkaStreams = KafkaStreams(topology, properties)
//
//        return RepositoryKStreams(streams)
//    }

    @Bean
    fun multiKafkaStreams(): RepositoryKStreams {
        val properties = Properties()
        properties.apply{
            put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

        val streamsBuilder = StreamsBuilder()

        // kafkastream app 1:
        streamsBuilder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues { textLine ->
                    textLine.toLowerCase()
                }
                .flatMapValues { loweredCase ->
                    loweredCase.split(" ")
                }
                .mapValues { splitText ->
                    println("app1: $splitText")
                    "app1: $splitText"
                }
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))

        // kafkastream app 2:
        streamsBuilder
                .stream(inputTopic2, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues { textLine ->
                    textLine.toLowerCase()
                }
                .flatMapValues { loweredCase ->
                    loweredCase.split(" ")
                }
                .mapValues { splitText ->
                    println("app2: $splitText")
                    "app2: $splitText"
                }
                .to(outputTopic2, Produced.with(Serdes.String(), Serdes.String()))

        val topology: Topology = streamsBuilder.build()
        val streams: KafkaStreams = KafkaStreams(topology, properties)

        return RepositoryKStreams(streams)
    }
}


