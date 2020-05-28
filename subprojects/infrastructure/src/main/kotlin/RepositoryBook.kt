package myapp.infrastructure

import myapp.domain.Book

class RepositoryBook {

    fun findAll(): List<Book>{
        return listOf(
                Book(23, "aaaa", "bbbb"),
                Book(24, "cccc", "dddd")
        )
    }
}
