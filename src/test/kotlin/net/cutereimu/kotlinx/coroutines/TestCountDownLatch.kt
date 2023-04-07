package net.cutereimu.kotlinx.coroutines

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger

class TestCountDownLatch {
    @Test
    fun testCountDownLatch() {
        runBlocking {
            val cd1 = CountDownLatch()
            val cd2 = CountDownLatch()
            (0 until 8).forEach { _ ->
                val n = 16
                cd1.countDown(-n)
                cd2.countDown(-n)
                val exited = Channel<Boolean>(n)
                for (i in 0 until n) {
                    launch {
                        cd1.countDown()
                        cd2.await()
                        exited.send(true)
                    }
                }
                cd1.await()
                (0 until n).forEach { _ ->
                    Assert.assertFalse(exited.tryReceive().isSuccess)
                    cd2.countDown()
                }
                (0 until n).forEach { _ -> exited.receive() }
                exited.close()
            }
        }
    }

    @Test
    fun testCountDownLatchMisuse() {
        try {
            val cd = CountDownLatch(1)
            cd.countDown(1)
            cd.countDown(1)
            throw Exception("should throw")
        } catch (e: IllegalArgumentException) {
            if (e.message != "sync: negative WaitGroup counter")
                throw e
        }
    }

    @Test
    fun testCountDownLatchRace() {
        runBlocking {
            (0 until 1000).forEach { _ ->
                val cd = CountDownLatch()
                val n = AtomicInteger()
                cd.countDown(-1)
                launch {
                    n.incrementAndGet()
                    cd.countDown()
                }
                cd.countDown(-1)
                launch {
                    n.incrementAndGet()
                    cd.countDown()
                }
                // Wait for goroutine 1 and 2
                cd.await()
                Assert.assertEquals(2, n.get())
            }
        }
    }
}