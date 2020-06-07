package myapp.test.domain

import java.time.Duration
import java.time.Instant
import java.util.*

class Faker {
    companion object {
        fun anyBoolean(): Boolean  = anyOf(listOf(true, false))

        fun anyInt(min: Int = -100000, max: Int = 100000): Int = (min..max).random()
        fun anyIntPositive(max: Int = 100000): Int = anyInt(min = 0, max = max)
        fun anyIntNegative(min: Int = -100000): Int = anyInt(min = min, max = 0)
        fun anyIntBetween(min: Int, max: Int): Int {
            val poolOptions = (min..max) as List<Int>
            return anyOf(poolOptions)
        }

        fun anyWord(maxLength: Int = 30): String {
            val charactersPool : List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9') + listOf('_', '-')
            val randomLength = anyInt(min = 2, max = maxLength)

            val randomTextAccumulator = ""
            (0..randomLength).forEach{
                randomTextAccumulator + anyOf(charactersPool)
            }

            return randomTextAccumulator
        }

        fun<T> anyOf(items: Collection<T>): T {
            val size = items.size
            val randomindex = anyIntPositive(size - 1)
            return items.elementAt(randomindex)
        }

        fun anyDateInPast(before: Date? = null): Date {
            var now = Instant.now()
            if (before !== null) {

            }
            return Date.from(now.minus(Duration.ofDays(anyIntPositive(max = 1000) as Long)))
        }

        fun anyDateInFuture(after: Date? = null): Date {
            var now = Instant.now()
            if (after !== null) {

            }

            return Date.from(now.plus(Duration.ofDays(anyIntPositive(max = 1000) as Long)))
        }

        fun anyDate(): Date = anyOf(listOf(anyDateInPast(), Date(), anyDateInFuture()))
    }
}
