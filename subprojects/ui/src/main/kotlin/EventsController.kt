package ui

import domain.LibraryEvent
import infrastructure.LibraryEventProducer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController


@RestController
class EventsController{

    @Autowired
    lateinit var libraryEventProducer: LibraryEventProducer


    @PostMapping("/v1/libraryevent")
    fun handler(@RequestBody libraryEvent: LibraryEvent): ResponseEntity<LibraryEvent>{
        libraryEventProducer.sendLibraryEventToCustomTopicUsingProducerRecord(libraryEvent)
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(libraryEvent)
    }
}
