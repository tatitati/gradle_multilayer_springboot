package myapp.test.domain

import java.time.Duration
import java.time.Instant
import java.util.*

class Faker {
    companion object {
        fun anyInt(min: Int = -100000, max: Int = 100000): Int = (min..max).random()
        fun anyIntPositive(max: Int = 100000): Int = anyInt(min = 0, max = max)

        fun <T> anyOf(items: Collection<T>): T {
            val size = items.size
            val randomindex = anyIntPositive(size - 1)
            return items.elementAt(randomindex)
        }

        fun <T> anyListFromBuilder(builder: () -> T, withMinLength: Int = 0, withMaxLength: Int = 10): List<T> {
            val amountItems = anyInt(min = withMinLength, max = withMaxLength)

            val items = mutableListOf<T>()
            for (i in 1..amountItems) {
                items.add(builder())
            }

            return items.toList()
        }

        fun anyString(withMaxLength: Int = 30, withMinLength: Int = 3, withCharactersPool: List<Char>? = null, allowEmpty: Boolean = false): String {
            val charactersPool: List<Char> = withCharactersPool
                    ?: ('a'..'z') + ('A'..'Z') + ('0'..'9') + listOf('.', '_', '-', ' ', '#', '!', '/', '\\')
            val randomLength = anyInt(min = withMinLength, max = withMaxLength)

            var randomTextAccumulator = ""
            (0..randomLength).forEach {
                randomTextAccumulator += anyOf(charactersPool)
            }

            if (allowEmpty) {
                return anyOf(listOf("", randomTextAccumulator))
            }

            return randomTextAccumulator
        }

        fun anyTopicName(): String {
            val charactersPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9') + listOf('.', '_', '-')
            return anyString(withMinLength = 5, withCharactersPool = charactersPool)
        }

        fun anyWord(allowEmpty: Boolean = false): String {
            val charactersPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
            return anyString(withMinLength = 4, withMaxLength = 30, withCharactersPool = charactersPool, allowEmpty = allowEmpty)
        }
    }
}
