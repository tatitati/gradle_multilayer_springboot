package myapp

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class AppConsumerProducer

fun main(args: Array<String>) {
    SpringApplication.run(AppConsumerProducer::class.java, *args)
}
