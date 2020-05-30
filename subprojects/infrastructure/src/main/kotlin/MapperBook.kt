package myapp.infrastructure

import com.fasterxml.jackson.databind.ObjectMapper
import myapp.domain.Book

class MapperBook {
    companion object{
        fun toJson(book: Book): String {
            val mapper = ObjectMapper()
            return mapper.writeValueAsString(book)
        }
    }
}
