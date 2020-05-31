package myapp.infrastructure.consumer

import myapp.infrastructure.MapperBook
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*

@Configuration
class FactoryRepositoryConsumer(
        @Value ("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
        @Value ("\${spring.kafka.consumer.key-deserializer}") private val keyDeserializer: String,
        @Value ("\${spring.kafka.consumer.value-deserializer}") private val valueDeserializer: String,
        @Value ("\${spring.kafka.consumer.group-id}") private val groupId: String
) {
    @Bean
    fun repositoryConsumer(): RepositoryConsumer {
        val properties = Properties()
        properties.apply{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        }

        val kConsumer = KafkaConsumer<String, String>(
                properties
        )

        return RepositoryConsumer(kConsumer, MapperBook())
    }
}


