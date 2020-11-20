package myapp.test.infrastructure.async

import kotlinx.coroutines.*
import org.junit.jupiter.api.Test

class Batch(){
    suspend fun start(){
        repeat(15) { i ->
            println("Processing $i ...")
            delay(300L)
        }
    }

    suspend fun pause(){
        repeat(15) { i ->
            println("paused")
            delay(300L)
        }
    }

    suspend fun close(){
        repeat(15) { i ->
            println("closed")
            delay(300L)
        }
    }
}

class LifeCycleTests {
    @Test
    fun `can start, pause and finish a batch processing`(){
        runBlocking {
            println("MAIN [${Thread.currentThread().name}] Launched coroutine")

            val jobStart = GlobalScope.launch(coroutineContext) {
                println("JOBSTART [${Thread.currentThread().name}] Launched coroutine")
                Batch().start()
            }
            println("running before waiting to complete start")
            jobStart.join() // wait for the coroutine to complete

            val jobPause = GlobalScope.launch(coroutineContext) {
                println("JOBPAUSE  [${Thread.currentThread().name}] Launched coroutine")
                Batch().pause()
            }
            println("running before waiting to complete pause")
            jobPause.join()
        }
        println("done")
    }
}
