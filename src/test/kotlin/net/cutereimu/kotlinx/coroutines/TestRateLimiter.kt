package net.cutereimu.kotlinx.coroutines

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import net.cutereimu.kotlinx.coroutines.RateLimiter.Reservation
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.math.abs
import kotlin.math.roundToInt
import kotlin.time.*
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalTime::class)
class TestRateLimiter {
    @Test
    fun testEvery() {
        fun closeEnough(a: Double, b: Double) = (abs(a / b) - 1.0) < 1e-9
        arrayOf(
            Duration.ZERO to RateLimiter.inf,
            (-1).nanoseconds to RateLimiter.inf,
            1.nanoseconds to 1e9,
            1.microseconds to 1e6,
            1.milliseconds to 1e3,
            10.milliseconds to 100.0,
            100.milliseconds to 10.0,
            1.seconds to 1.0,
            2.seconds to 0.5,
            2.5.seconds to 0.4,
            4.seconds to 0.25,
            10.seconds to 0.1
        ).forEach { (interval, limit) -> Assert.assertTrue(closeEnough(every(interval), limit)) }
    }

    private val d = 100.milliseconds
    private val t0 = TimeSource.Monotonic.markNow()
    private val t1 = t0 + d * 1
    private val t2 = t0 + d * 2
    private val t3 = t0 + d * 3
    private val t4 = t0 + d * 4
    private val t5 = t0 + d * 5
    private val t9 = t0 + d * 9

    private class Allow(
        val t: ComparableTimeMark,
        val toks: Double,
        val n: Int,
        val ok: Boolean
    )

    private suspend fun run(limiter: RateLimiter, vararg allows: Allow) {
        for ((i, allow) in allows.withIndex()) {
            println("step $i: ${allow.t - t0} later want ${allow.toks}")
            Assert.assertEquals(allow.toks, limiter.getTokens(allow.t), 0.0)
            Assert.assertEquals(allow.ok, limiter.allow(allow.n, allow.t))
        }
    }

    @Test
    fun testLimiterBurst1() {
        runBlocking {
            run(
                RateLimiter(10.0, 1),
                Allow(t0, 1.0, 1, true),
                Allow(t0, 0.0, 1, false),
                Allow(t0, 0.0, 1, false),
                Allow(t1, 1.0, 1, true),
                Allow(t1, 0.0, 1, false),
                Allow(t1, 0.0, 1, false),
                Allow(t2, 1.0, 2, false), // burst size is 1, so n=2 always fails
                Allow(t2, 1.0, 1, true),
                Allow(t2, 0.0, 1, false)
            )
        }
    }

    @Test
    fun testLimiterBurst3() {
        runBlocking {
            run(
                RateLimiter(10.0, 3),
                Allow(t0, 3.0, 2, true),
                Allow(t0, 1.0, 2, false),
                Allow(t0, 1.0, 1, true),
                Allow(t0, 0.0, 1, false),
                Allow(t1, 1.0, 4, false),
                Allow(t2, 2.0, 1, true),
                Allow(t3, 2.0, 1, true),
                Allow(t4, 2.0, 1, true),
                Allow(t4, 1.0, 1, true)
            )
        }
    }

    @Test
    fun testLimiterJumpBackwards() {
        runBlocking {
            run(
                RateLimiter(10.0, 3),
                Allow(t1, 3.0, 1, true), // start at t1
                Allow(t0, 2.0, 1, true), // jump back to t0, two tokens remain
                Allow(t0, 1.0, 1, true),
                Allow(t0, 0.0, 1, false),
                Allow(t0, 0.0, 1, false),
                Allow(t1, 1.0, 1, true), // got a token
                Allow(t1, 0.0, 1, false),
                Allow(t1, 0.0, 1, false),
                Allow(t2, 1.0, 1, true), // got another token
            )
        }
    }

    @Test
    fun testLimiter_noTruncationErrors() {
        runBlocking {
            Assert.assertTrue(RateLimiter(0.7692307692307693, 1).allow())
        }
    }

    @Test
    fun testSimultaneousRequests() {
        runBlocking {
            val limit = 1.0
            val burst = 5
            val numRequests = 15
            val numOK = AtomicInteger()
            val limiter = RateLimiter(limit, burst)
            val jobs = (0 until numRequests).map {
                launch {
                    if (limiter.allow()) numOK.incrementAndGet()
                }
            }
            jobs.joinAll()
            Assert.assertEquals(burst, numOK.get())
        }
    }

    @Test
    fun testLongRunningQPS() {
        runBlocking {
            val limit = 100.0
            val burst = 100
            var numOK = 0
            val limiter = RateLimiter(limit, burst)
            val start = TimeSource.Monotonic.markNow()
            val end = start + 5.seconds
            while (TimeSource.Monotonic.markNow() < end) {
                if (limiter.allow())
                    numOK++
                delay(2.milliseconds)
            }
            val elapsed = start.elapsedNow()
            val ideal = burst + (elapsed * limit).toInt(DurationUnit.SECONDS)
            Assert.assertTrue(numOK <= ideal + 1)
            Assert.assertTrue(numOK >= (0.999 * ideal).toInt())
        }
    }

    private class Request(
        val t: ComparableTimeMark,
        val n: Int,
        val act: ComparableTimeMark,
        val ok: Boolean
    )

    private fun dFromDuration(dur: Duration): Int = (dur / d).roundToInt()
    private fun dSince(t: ComparableTimeMark): Int = dFromDuration(t - t0)

    private suspend fun runReserve(
        limiter: RateLimiter,
        req: Request,
        maxReserve: Duration = RateLimiter.infDuration
    ): Reservation {
        val r = limiter.reserve(req.n, maxReserve, req.t)
        if (r.ok) Assert.assertEquals(dSince(r.timeToAct), dSince(req.act))
        Assert.assertEquals(req.ok, r.ok)
        return r
    }

    @Test
    fun testSimpleReserve() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            runReserve(limiter, Request(t0, 2, t2, true))
            runReserve(limiter, Request(t3, 2, t4, true))
        }
    }

    @Test
    fun testMix() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t0, 3, t1, false)) // should return false because n > Burst
            runReserve(limiter, Request(t0, 2, t0, true))
            run(limiter, Allow(t1, 1.0, 2, false)) // not enough tokens - don't allow
            runReserve(limiter, Request(t1, 2, t2, true))
            run(limiter, Allow(t1, -1.0, 1, false)) // negative tokens - don't allow
            run(limiter, Allow(t3, 1.0, 1, true))
        }
    }

    @Test
    fun testCancelInvalid() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            val r = runReserve(limiter, Request(t0, 3, t3, false))
            r.cancel(at = t0) // should have no effect
            runReserve(limiter, Request(t0, 2, t2, true)) // did not get extra tokens
        }
    }

    @Test
    fun testCancelLast() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            val r = runReserve(limiter, Request(t0, 2, t2, true))
            r.cancel(at = t1) // got 2 tokens back
            runReserve(limiter, Request(t1, 2, t2, true))
        }
    }

    @Test
    fun testCancelTooLate() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            val r = runReserve(limiter, Request(t0, 2, t2, true))
            r.cancel(at = t3) // too late to cancel - should have no effect
            runReserve(limiter, Request(t3, 2, t4, true))
        }
    }

    @Test
    fun testCancel0Tokens() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            val r = runReserve(limiter, Request(t0, 1, t1, true))
            runReserve(limiter, Request(t0, 1, t2, true))
            r.cancel(at = t0) // got 0 tokens back
            runReserve(limiter, Request(t0, 1, t3, true))
        }
    }

    @Test
    fun testCancel1Tokens() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            val r = runReserve(limiter, Request(t0, 2, t2, true))
            runReserve(limiter, Request(t0, 1, t3, true))
            r.cancel(at = t2) // got 1 token back
            runReserve(limiter, Request(t2, 2, t4, true))
        }
    }

    @Test
    fun testCancelMulti() {
        runBlocking {
            val limiter = RateLimiter(10.0, 4)
            runReserve(limiter, Request(t0, 4, t0, true))
            val rA = runReserve(limiter, Request(t0, 3, t3, true))
            runReserve(limiter, Request(t0, 1, t4, true))
            val rC = runReserve(limiter, Request(t0, 1, t5, true))
            rC.cancel(at = t1) // get 1 token back
            rA.cancel(at = t1) // get 2 tokens back, as if C was never reserved
            runReserve(limiter, Request(t1, 3, t5, true))
        }
    }

    @Test
    fun testReserveJumpBack() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t1, 2, t1, true)) // start at t1
            runReserve(limiter, Request(t0, 1, t1, true)) // should violate Limit,Burst
            runReserve(limiter, Request(t2, 2, t3, true))
        }
    }

    @Test
    fun testReserveJumpBackCancel() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t1, 2, t1, true)) // start at t1
            val r = runReserve(limiter, Request(t1, 2, t3, true))
            runReserve(limiter, Request(t1, 1, t4, true))
            r.cancel(at = t0) // cancel at t0, get 1 token back
            runReserve(limiter, Request(t1, 2, t4, true)) // should violate Limit,Burst
        }
    }

    @Test
    fun testReserveSetLimit() {
        runBlocking {
            val limiter = RateLimiter(5.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            runReserve(limiter, Request(t0, 2, t4, true))
            limiter.setLimit(10.0, at = t2)
            runReserve(limiter, Request(t2, 1, t4, true)) // violates Limit and Burst
        }
    }

    @Test
    fun testReserveSetBurst() {
        runBlocking {
            val limiter = RateLimiter(5.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            runReserve(limiter, Request(t0, 2, t4, true))
            limiter.setBurst(4, at = t3)
            runReserve(limiter, Request(t0, 4, t9, true)) // violates Limit and Burst
        }
    }

    @Test
    fun testReserveSetLimitCancel() {
        runBlocking {
            val limiter = RateLimiter(5.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true))
            val r = runReserve(limiter, Request(t0, 2, t4, true))
            limiter.setLimit(10.0, at = t2)
            r.cancel(t2) // 2 tokens back
            runReserve(limiter, Request(t2, 2, t3, true))
        }
    }

    @Test
    fun testReserveMax() {
        runBlocking {
            val limiter = RateLimiter(10.0, 2)
            runReserve(limiter, Request(t0, 2, t0, true), d)
            runReserve(limiter, Request(t0, 1, t1, true), d) // reserve for close future
            runReserve(limiter, Request(t0, 1, t2, false), d) // time to act too far in the future
        }
    }

    private class Wait(
        val name: String,
        val ctx: CoroutineContext,
        val n: Int,
        val delay: Int, // in multiples of d
        val noException: Boolean
    )

    private suspend fun runWait(limiter: RateLimiter, w: Wait) {
        println("testing ${w.name}")
        val start = TimeSource.Monotonic.markNow()
        try {
            limiter.wait(w.ctx, w.n, start)
            Assert.assertTrue(w.noException)
        } catch (_: Exception) { // Assert.asserTrue 抛出的是Error不是Exception
            Assert.assertFalse(w.noException)
        }
        val delay = start.elapsedNow()
        Assert.assertTrue(waitDelayOk(w.delay, delay))
    }

    private fun waitDelayOk(wantD: Int, got: Duration): Boolean {
        val gotD = dFromDuration(got)
        if (gotD < wantD)
            return false
        val maxD = (wantD * 3 + 1) / 2
        return gotD <= maxD
    }

    private suspend fun newFailedContext(scope: CoroutineScope): CoroutineContext {
        var context: CoroutineContext? = null
        val ch = Channel<Boolean>(1)
        val job = scope.launch {
            context = this.coroutineContext
            ch.send(true)
        }
        ch.receive()
        job.cancelAndJoin()
        return context!!
    }

    @Test
    fun testWaitSimple() {
        runBlocking {
            val limiter = RateLimiter(10.0, 3)
            runWait(limiter, Wait("not-active", newFailedContext(this), 1, 0, false))
            runWait(limiter, Wait("exceed-burst-error", coroutineContext, 4, 0, false))
            runWait(limiter, Wait("act-now", coroutineContext, 2, 0, true))
            runWait(limiter, Wait("act-later", coroutineContext, 3, 2, true))
        }
    }

    @Test
    fun testWaitCancel() {
        runBlocking {
            val limiter = RateLimiter(10.0, 3)
            runWait(limiter, Wait("act-now", coroutineContext, 2, 0, true))
            val job = launch {
                runWait(limiter, Wait("will-cancel", coroutineContext, 3, 1, false))
            }
            delay(d)
            job.cancelAndJoin()
            println("tokens:${limiter.tokens} last:${limiter.last} lastEvent:${limiter.lastEvent}")
            runWait(limiter, Wait("act-now-after-cancel", coroutineContext, 2, 0, true))
        }
    }

    @Test
    fun testWaitTimeout() {
        runBlocking {
            val limiter = RateLimiter(10.0, 3)
            val job = launch {
                runWait(limiter, Wait("act-now", coroutineContext, 2, 0, true))
                runWait(limiter, Wait("w-timeout-err", coroutineContext, 3, 1, false))
            }
            delay(d)
            job.cancelAndJoin()
        }
    }

    @Test
    fun testWaitInf() {
        runBlocking {
            val limiter = RateLimiter(RateLimiter.inf, 0)
            runWait(limiter, Wait("exceed-burst-no-error", coroutineContext, 3, 0, true))
        }
    }

    @Test
    fun testZeroLimit() {
        runBlocking {
            val limiter = RateLimiter(0.0, 1)
            Assert.assertTrue(limiter.allow())
            Assert.assertFalse(limiter.allow())
        }
    }
}