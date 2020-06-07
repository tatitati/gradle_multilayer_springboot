package myapp.presentation.web

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import myapp.applicatin.ServiceBook
import myapp.domain.Book
import myapp.infrastructure.consumer.RepositoryConsumer
import myapp.infrastructure.kafkastream.RepositoryKStreams
import myapp.infrastructure.producer.RepositoryProducer

// Examples:
// localhost:8081/hello
// localhost:8081/consume
// localhost:8081/produce
// localhost:8081/streams

@RestController
class ControllerBook @Autowired constructor(
        val serviceBook: ServiceBook,
        val repositoryConsumer: RepositoryConsumer,
        val repositoryProducer: RepositoryProducer,
        val repositoryKStreams: RepositoryKStreams
){
    @GetMapping("/hello")
    fun helloKotlin(): String {
        val books: List<Book> = serviceBook.findAllBooks()
        return books.toString()
    }

    @GetMapping("/consume")
    fun consume(): Unit {
        repositoryConsumer.consumeFromTopic("mytopic")
    }

    @GetMapping("/produce")
    fun produce(): Unit {
        (1..20).forEach{
            val book = Book(it, "aaaa_" + it, "bbbbb_" + it)
            repositoryProducer.sendToTopic(book, "mytopic")
        }

    }

    @GetMapping("/streams")
    fun kstream(): Unit {
        repositoryKStreams.start()

        (1..20).forEach{
            val book = Book(it, "aaaa_" + it, "bbbbb_" + it)
            repositoryProducer.sendToTopic(book, "word-count-input")

            val book2 = Book(it, "cccc_" + it, "ddddd_" + it)
            repositoryProducer.sendToTopic(book2, "word-count-input2")
        }
    }
}
