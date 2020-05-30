package myapp.infrastructure.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*

@Configuration
class Configuration(
        @Value ("\${spring.kafka.consumer.bootstrap-servers}") private val bootstrapServers: String,
        @Value ("\${spring.kafka.consumer.key-deserializer}") private val keyDeserializer: String,
        @Value ("\${spring.kafka.consumer.value-deserializer}") private val valueDeserializer: String,
        @Value ("\${spring.kafka.consumer.group-id}") private val groupId: String
) {
    @Bean
    fun repository(): Repository {
        val prop = Properties()
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

        val consumer = KafkaConsumer<String, String>(
                prop
        )

        val repoC = Repository(consumer)
        return repoC

    }
}


