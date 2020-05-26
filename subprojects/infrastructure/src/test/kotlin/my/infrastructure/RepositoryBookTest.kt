package my.infrastructure

import org.junit.jupiter.api.Test

class RepositoryBookTest {

    @Test
    fun `RepositoryBook finaAll returns two books`(){
        assert(2 == RepositoryBook().finAll().size)
    }
}
