package net.cutereimu.kotlinx.coroutines

import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.atomic.AtomicLong

class CountDownLatch(count: Int) {
    private val state = AtomicLong(count.toLong() shl 32)
    private val sema = Semaphore(count, count)

    @Throws(InterruptedException::class)
    suspend fun await() {
        while (true) {
            val state = this.state.get()
            val v = (state shr 32).toInt()
            if (v == 0)
                return
            if (this.state.compareAndSet(state, state + 1)) {
                sema.acquire()
                if (this.state.get() != 0L)
                    throw IllegalStateException("sync: WaitGroup is reused before previous Wait has returned")
                return
            }
        }
    }

    fun countDown(count: Int = 1) {
        val delta = -count
        val state = this.state.addAndGet(delta.toLong() shl 32)
        val v = (state shr 32).toInt()
        val w = state.toUInt()
        if (v < 0)
            throw IllegalArgumentException("sync: negative WaitGroup counter")
        if (w != 0U && delta > 0 && v == delta)
            throw IllegalStateException("sync: WaitGroup misuse: Add called concurrently with Wait")
        if (v > 0 || w == 0U)
            return
        if (this.state.get() != state)
            throw IllegalStateException("sync: WaitGroup misuse: Add called concurrently with Wait")
        this.state.set(0)
        (w downTo 1U).forEach { _ -> sema.release() }
    }

    val count: Long = state.get() shr 32

    override fun toString(): String = "${super.toString()}[Count = $count]"
}