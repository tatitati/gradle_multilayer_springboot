package myapp.infrastructure

import org.springframework.stereotype.Repository
import myapp.domain.Book

@Repository
class RepositoryBook {

    fun findAll(): List<Book>{
        return listOf(
                Book(23, "aaaa", "bbbb"),
                Book(24, "cccc", "dddd")
        )
    }
}
