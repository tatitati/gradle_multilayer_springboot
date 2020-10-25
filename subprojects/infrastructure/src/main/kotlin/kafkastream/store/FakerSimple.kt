package myapp.infrastructure.kafkastream.store

import java.time.Duration
import java.time.Instant
import java.util.*

class FakerSimple {
    companion object {
        fun anyInt(min: Int = -100000, max: Int = 100000): Int = (min..max).random()
        fun anyIntPositive(max: Int = 100000): Int = anyInt(min = 0, max = max)

        fun<T> anyOf(items: Collection<T>): T {
            val size = items.size
            val randomindex = anyIntPositive(size - 1)
            return items.elementAt(randomindex)
        }
    }
}
