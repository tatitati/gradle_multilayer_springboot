package myapp.infrastructure

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import java.util.*

class ConfigConsumer(
        private val bootstrapServers: String,
        private val keyDeserializer: String,
        private val valueDeserializer: String,
        private val groupId: String
): Properties()

@Configuration
@ConfigurationProperties(prefix = "spring.kafka.consumer")
class Repo() {
    lateinit var bootstrapServers: String
    lateinit var keyDeserializer: String
    lateinit var valueDeserializer: String
    lateinit  var groupId: String

    @Bean
    fun repository(): RepositoryConsumer {
        val myconfig = ConfigConsumer(
                this.bootstrapServers,
                this.keyDeserializer,
                this.valueDeserializer,
                this.groupId
        )

        val consumer = KafkaConsumer<String, String>(
                myconfig
        )

        val repoC = RepositoryConsumer(consumer)
        return repoC

    }
}


