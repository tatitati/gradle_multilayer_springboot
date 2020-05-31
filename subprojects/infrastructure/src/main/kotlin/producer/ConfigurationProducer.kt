package myapp.infrastructure.producer

import myapp.infrastructure.MapperBook
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*

@Configuration
class ConfigurationProducer(
        @Value ("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
        @Value ("\${spring.kafka.producer.key-serializer}") private val keySerializer: String,
        @Value ("\${spring.kafka.producer.value-serializer}") private val valueSerializer: String
) {
    @Bean
    fun repositoryProducer(): RepositoryProducer {
        val properties = Properties()
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)

        val kProducer = KafkaProducer<String, String>(
                properties
        )

        return RepositoryProducer(kProducer, MapperBook())
    }
}


