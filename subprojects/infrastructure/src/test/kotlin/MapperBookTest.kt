package myapp.test.infrastructure

import com.fasterxml.jackson.databind.ObjectMapper
import myapp.domain.Book
import myapp.infrastructure.MapperBook
import org.junit.jupiter.api.Test

class MapperBookTest {
    @Test
    fun bookToJson(){
        val givenBook = Book(23, "asdf", "asdfasdf")

        val json = MapperBook().toJson(givenBook)

        assert("""{"bookId":23,"bookName":"asdf","bookAuthor":"asdfasdf"}""" == json)
    }

    @Test
    fun jsonToBook(){
        val givenJson = """{"bookId":23,"bookName":"asdf","bookAuthor":"asdfasdf"}"""

        val book = MapperBook().toBook(givenJson)

        assert(book == Book(23, "asdf", "asdfasdf"))
    }

    @Test
    fun canUseBuildersInDomain(){

    }
}
