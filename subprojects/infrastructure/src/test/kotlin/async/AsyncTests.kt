package myapp.test.infrastructure.async

import org.junit.jupiter.api.Test
import kotlinx.coroutines.*

class AsyncTests {
    @Test
    fun `Cancelling a coroutine execution`(){
        runBlocking {
            val job = GlobalScope.launch {
                repeat(30) { i ->
                    println("Processing $i ...")
                    delay(300L)
                }
            }
            delay(1000L)
            println("main: The user requests the cancellation of the processing")
            job.cancelAndJoin() // cancel the job, it doesnt wait to complete the coroutine
            println("main: The batch processing is cancelled")
        }
        println("done")

        // Processing 0 ...
        // Processing 1 ...
        // Processing 2 ...
        // Processing 3 ...
        // main: The user requests the cancellation of the processing
        // main: The batch processing is cancelled
        // done
    }

    @Test
    fun `I can use the same than before, using a suspend function, same logic`(){
        runBlocking {
            val job = GlobalScope.launch {
                repeat(30) { i ->
                    processBatch(i)
                }
            }
            delay(1000L)
            println("main: The user requests the cancellation of the processing")
            job.cancelAndJoin() // cancel the job, it doesnt wait to complete the coroutine
            println("main: The batch processing is cancelled")
        }
        println("done")

        // Processing 0 ...
        // Processing 1 ...
        // Processing 2 ...
        // Processing 3 ...
        // main: The user requests the cancellation of the processing
        // main: The batch processing is cancelled
        // done
    }

    private suspend fun processBatch(i: Int) {
        println("Processing $i ...")
        delay(300L)
    }


    @Test
    fun `joining a coroutine execution`(){
        runBlocking {
            val job = GlobalScope.launch {
                repeat(30) { i ->
                    println("Processing $i ...")
                    delay(300L)
                }
            }
            println("main: The user requests the cancellation of the processing")
            job.join() // wait for the coroutine to complete
            println("main: The batch processing is cancelled")
        }
        println("done")

        // main: The user requests the cancellation of the processing
        // Processing 0 ...
        // Processing 1 ...
        // Processing 2 ...
        // Processing 3 ...
        // Processing 4 ...
        // Processing 5 ...
        // Processing 6 ...
        // Processing 7 ...
        // Processing 8 ...
        // Processing 9 ...
        // Processing 10 ...
        // Processing 11 ...
        // Processing 12 ...
        // Processing 13 ...
        // Processing 14 ...
        // Processing 15 ...
        // Processing 16 ...
        // Processing 17 ...
        // Processing 18 ...
        // Processing 19 ...
        // Processing 20 ...
        // Processing 21 ...
        // Processing 22 ...
        // Processing 23 ...
        // Processing 24 ...
        // Processing 25 ...
        // Processing 26 ...
        // Processing 27 ...
        // Processing 28 ...
        // Processing 29 ...
        // main: The batch processing is cancelled
        // done
        // BUILD
    }

    @Test
    fun `async and deferred`() {
        runBlocking {
            val deferred: Deferred<Int> = GlobalScope.async {
                // some business logic
                delay(300)
                return@async 4
            }

            println("deferred is running while....")
            val result: Int = deferred.await()
            println(result)

            // deferred is running while....
            // 4
        }
    }
}

