package my.infrastructure

import my.domain.Book


class RepositoryBook {

    fun finAll(): List<Book>{
        return listOf(
                Book(23, "aaaa", "bbbb"),
                Book(24, "cccc", "dddd")
        )
    }
}
