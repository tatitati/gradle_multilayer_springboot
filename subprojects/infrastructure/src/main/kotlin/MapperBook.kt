package myapp.infrastructure

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import myapp.domain.Book

class MapperBook {
    companion object{
        fun toJson(book: Book): String {
            val mapper = jacksonObjectMapper()
            return mapper.writeValueAsString(book)
        }

        fun toBook(json: String): Book {
            val mapper = jacksonObjectMapper()
            return mapper.readValue<Book>(json)
        }
    }
}
