package net.cutereimu.kotlinx.coroutines

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import kotlin.random.Random

class TestConcurrentHashMap {
    @Test
    fun testConcurrentForeach() {
        runBlocking {
            val mapSize = 1L shl 10

            val m = ConcurrentHashMap<Long, Long>()
            (1L..mapSize).forEach { n -> m.put(n, n) }

            val done = Channel<Boolean>()
            val cd = CountDownLatch()
            try {
                (Runtime.getRuntime().availableProcessors() downTo 1).forEach { g ->
                    val r = Random(g)
                    cd.countDown(-1)
                    launch {
                        try {
                            var i = 0L
                            while (true) {
                                if (done.tryReceive().isClosed) return@launch
                                (1 until mapSize).forEach { n ->
                                    if (r.nextLong(mapSize) == 0L)
                                        m.put(n, n * i * g)
                                    else
                                        m.get(n)
                                }
                                i++
                            }
                        } finally {
                            cd.countDown()
                        }
                    }
                }

                (1 shl 10 downTo 1).forEach { _ ->
                    val seen = HashMap<Long, Boolean>(mapSize.toInt())

                    m.forEach { (k, v) ->
                        Assertions.assertEquals(0L, v % k)
                        Assertions.assertFalse(seen.containsKey(k))
                        seen[k] = true
                    }

                    Assertions.assertEquals(mapSize.toInt(), seen.size)
                }
            } finally {
                done.close()
                cd.await()
            }
        }
    }

    @Test
    fun testMapRangeNestedCall() {
        runBlocking {
            val m = ConcurrentHashMap<Int, String>()
            for ((i, v) in arrayOf("hello", "world", "Go").withIndex()) {
                m.put(i, v)
            }
            m.forEach { entry ->
                m.forEach { (key, value) ->
                    val v = m.get(key)
                    Assertions.assertNotEquals(null, v)
                    Assertions.assertEquals(value, v)
                    m.getOrPut(42) { "dummy" }
                    m.put(42, "sync.Map")
                    val v2 = m.remove(42)
                    Assertions.assertNotEquals(null, v2)
                    Assertions.assertEquals("sync.Map", v2)
                }
                m.remove(entry.key)
            }

            var length = 0
            m.forEach { _ -> length++ }
            Assertions.assertEquals(0, length)
        }
    }
}