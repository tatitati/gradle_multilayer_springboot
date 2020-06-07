package myapp.infrastructure

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import myapp.domain.Book

class MapperBook {
    fun toJson(book: Book): String {
        return jacksonObjectMapper().writeValueAsString(book)
    }

    fun toBook(json: String): Book {
        return jacksonObjectMapper().readValue<Book>(json)
    }
}
