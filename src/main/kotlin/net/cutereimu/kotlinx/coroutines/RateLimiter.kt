package net.cutereimu.kotlinx.coroutines

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.cutereimu.kotlinx.coroutines.RateLimiter.Reservation
import kotlin.coroutines.CoroutineContext
import kotlin.math.min
import kotlin.time.*
import kotlin.time.Duration.Companion.seconds

/** 用来表示事件频率的限制，即每秒多少个事件。0表示不允许任何事件 */
typealias Limit = Double

/**
 * RateLimiter 用来控制事件发生的频率。它实现了一个大小为b的“令牌桶”，最初是满的，然后以每秒r个令牌的速率重新填充。
 *
 * 换句话讲，在足够大的时间间隔内，RateLimiter 将速率限制为每秒r个令牌，最大突发大小为b个事件。
 *
 * 特别地，如果 r == [RateLimiter.inf] （无限速率），则b可以忽略。
 *
 * RateLimiter 主要有三个方法[allow]，[reserve]，[wait]。在大多数情况下会使用[wait]。
 *
 * 当没有令牌可以获得时，它们的表现有所不同：
 * * [allow]将返回`false`
 * * [reserve]将返回一个将来令牌的[Reservation]，调用[Reservation.delay]可以获得需要等待的时间
 * * [wait]将会持续阻塞，直到有足够的令牌可以获取
 * @param limit 总体事件最大速率。换言之，每隔 `1/limit` 秒恢复一个令牌
 * @param burst 突发事件个数。即为令牌个数上限
 * @see every
 */
@ExperimentalTime
class RateLimiter(
    private var limit: Limit = 0.0,
    private var burst: Int = 0
) {
    private val mu = Mutex()

    /** 当前令牌个数 */
    internal var tokens = burst.toDouble()

    /** 最后更新令牌的时间 */
    internal var last: ComparableTimeMark = TimeSource.Monotonic.markNow()

    /** 最近一次事件的时间（过去或将来） */
    internal var lastEvent = last

    /** @return 总体事件最大速率。换言之，每隔 `1/limit` 秒恢复一个令牌 */
    suspend fun getLimit(): Limit = mu.withLock { limit }

    /** @return 突发事件个数。即为令牌个数上限 */
    suspend fun getBurst(): Int = mu.withLock { burst }

    /** 获取某时间的令牌数量。默认获取当前的令牌数量。 */
    suspend fun getTokens(at: ComparableTimeMark = TimeSource.Monotonic.markNow()): Double =
        mu.withLock { advance(at).third }

    /**
     * 判断在某时刻是否允许n个事件发生。如果返回，则同时会消耗掉n个令牌。
     *
     * 在您打算丢弃超出限制的事件时，可以考虑使用这个方法。
     * @see reserve
     * @see wait
     */
    suspend fun allow(n: Int = 1, t: ComparableTimeMark = TimeSource.Monotonic.markNow()): Boolean {
        return reserve(n, Duration.ZERO, t).ok
    }

    /**
     * 如果您希望在不丢弃事件的情况下根据速率限制进行等待和减速，请使用此方法。
     *
     * 用法示例：
     * ```
     * val lim = RateLimiter(1.0, 10)
     * val r = lim.reserve()
     * if (!r.ok)
     *     return
     * delay(r.delay() / 1000000L)
     * // doSth
     * ```
     * @param n 保留的事件个数
     * @return 一个[Reservation]，该[Reservation]指示调用者在n个事件发生之前必须等待多长时间。
     * 在允许未来事件时，会将此[Reservation]考虑在内。
     * 如果参数[n]超过限制器的突发大小，则返回的Reservation的OK（）方法返回false。
     * @see allow
     * @see wait
     */
    suspend fun reserve(n: Int = 1, t: ComparableTimeMark = TimeSource.Monotonic.markNow()): Reservation {
        return reserve(n, infDuration, t)
    }

    /**
     * 阻塞直到允许n个事件发生（有n个令牌）。阻塞结束后，n个令牌会被消耗。若中途ctx被cancel，则会尽可能返还令牌。
     * @throws IllegalArgumentException n超过了[burst]值，除非[limit]为[RateLimiter.inf]
     * @throws CancellationException 协程被取消
     * @see allow
     * @see reserve
     */
    suspend fun wait(ctx: CoroutineContext, n: Int = 1) {
        wait(ctx, n, TimeSource.Monotonic.markNow())
    }

    internal suspend fun wait(
        ctx: CoroutineContext,
        n: Int,
        t: ComparableTimeMark,
        newTimer: suspend (Duration) -> (suspend () -> Unit) = { { delay(it) } },
    ) {
        val (burst, limit) = mu.withLock { Pair(this.burst, this.limit) }

        if (n > burst && limit != inf)
            throw IllegalArgumentException("rate: Wait(n=$n) exceeds limiter's burst $burst")
        if (!ctx.isActive)
            throw CancellationException("current coroutine is not active")
        val waitLimit = infDuration
        val r = reserve(n, waitLimit, t)
        if (!r.ok)
            throw IllegalStateException("rate: Wait(n=$n) would exceed context deadline")
        val delay = r.delay(t)
        if (delay == Duration.ZERO)
            return
        try {
            newTimer(delay)()
        } catch (e: CancellationException) {
            withContext(NonCancellable) {
                r.cancel()
                throw e
            }
        }
    }

    suspend fun setLimit(newLimit: Limit, at: ComparableTimeMark = TimeSource.Monotonic.markNow()) {
        mu.withLock {
            val (t, _, tokens) = advance(at)
            this.last = t
            this.tokens = tokens
            this.limit = newLimit
        }
    }

    suspend fun setBurst(newBurst: Int, at: ComparableTimeMark = TimeSource.Monotonic.markNow()) {
        mu.withLock {
            val (t, _, tokens) = advance(at)
            this.last = t
            this.tokens = tokens
            this.burst = newBurst
        }
    }

    /**
     * @param maxFutureReserve 允许的最大保留等待时间
     */
    internal suspend fun reserve(n: Int, maxFutureReserve: Duration, t: ComparableTimeMark): Reservation {
        mu.withLock {
            if (limit == inf) {
                return Reservation(true, this, n, t)
            } else if (limit == 0.0) {
                val ok = burst >= n
                if (ok) burst -= n
                return Reservation(ok, this, burst, t)
            }

            var (newT, last, tokens) = advance(t)
            tokens -= n
            val waitDuration = if (tokens < 0) limit.durationFromTokens(-tokens) else Duration.ZERO
            val ok = n <= burst && waitDuration <= maxFutureReserve
            val timeToAct = if (ok) newT + waitDuration else newT
            val r = Reservation(
                ok = ok,
                limiter = this,
                limit = limit,
                tokens = if (ok) n else 0,
                timeToAct = timeToAct
            )
            if (ok) {
                this.last = newT
                this.tokens = tokens
                this.lastEvent = timeToAct
            } else {
                this.last = last
            }
            return r
        }
    }

    /**
     * 根据时间的流逝计算新状态。仅计算并返回，不会修改。调用此方法前请先确保[mu]已经获取了锁。
     * @return (newT, new[last][RateLimiter.last], new[tokens])
     */
    private fun advance(t: ComparableTimeMark = TimeSource.Monotonic.markNow()): Triple<ComparableTimeMark, ComparableTimeMark, Double> {
        val last = this.last.coerceAtMost(t)
        val elapsed = t - last
        val delta = limit.tokensFromDuration(elapsed)
        val tokens = min(this.tokens + delta, burst.toDouble())
        return Triple(t, last, tokens)
    }

    /** 保存有关[RateLimiter]允许在延迟后发生的事件的信息。它可以被[cancel]，这将使[RateLimiter]允许更多事件 */
    class Reservation(
        val ok: Boolean,
        private val limiter: RateLimiter,
        private val tokens: Int,
        internal val timeToAct: ComparableTimeMark,
        private val limit: Limit = 0.0
    ) {
        /**
         * @return 保留持有者在执行保留操作之前必须等待的持续时间。
         * 0表示立即执行。
         * [infDuration]表示[RateLimiter]无法在最长等待时间内授予此保留中请求的令牌。
         */
        fun delay(from: ComparableTimeMark = TimeSource.Monotonic.markNow()): Duration {
            if (!ok) return infDuration
            return (timeToAct - from).coerceAtLeast(Duration.ZERO)
        }

        /** 表示调用者不再执行保留操作。考虑到可能已经进行了其他保留，尽可能地返还本预订对[RateLimiter]的影响。 */
        suspend fun cancel(at: ComparableTimeMark = TimeSource.Monotonic.markNow()) {
            if (!ok) return
            limiter.mu.withLock {
                if (limiter.limit == inf || tokens == 0 || timeToAct < at) return
                // 计算返还的令牌数
                // 通过limiter.lastEvent和this.timeToAct可以得出在本保留被获取后有多少令牌已经保留了。这些令牌不能被回收。
                val restoreTokens = tokens.toDouble() - limit.tokensFromDuration(limiter.lastEvent - timeToAct)
                if (restoreTokens <= 0) return
                var (t, _, tokens) = limiter.advance(at)
                tokens += restoreTokens
                tokens = min(tokens, limiter.burst.toDouble())
                limiter.last = t
                limiter.tokens = tokens
                if (timeToAct == limiter.lastEvent) {
                    val prevEvent = timeToAct + limit.durationFromTokens((-this.tokens).toDouble())
                    if (prevEvent >= t)
                        limiter.lastEvent = prevEvent
                }
            }
        }
    }

    companion object {
        /**
         * 用来表示无限速率，允许所有事件（即使 [burst] 为0）
         * @see limit
         */
        const val inf: Limit = Double.MAX_VALUE

        val infDuration: Duration = Duration.INFINITE

        /**
         * @param tokens 令牌数量
         * @return 以该速率累积 `tokens` 个令牌所需的持续时间
         */
        private fun Limit.durationFromTokens(tokens: Double): Duration {
            if (this <= 0) return infDuration
            val seconds = tokens / this
            return seconds.seconds
        }

        /**
         * @param d 时间
         * @return 以该速率经过`d`时间累积的令牌数
         */
        private fun Limit.tokensFromDuration(d: Duration): Double {
            if (this <= 0) return 0.0
            return d.toDouble(DurationUnit.SECONDS) * this
        }
    }
}

/**
 * 用来将每隔`x`秒一个令牌转化为每秒`1/x`个令牌
 * @param interval 生成令牌的间隔
 * @return 每秒多少个令牌
 */
@ExperimentalTime
fun every(interval: Duration): Limit =
    if (!interval.isPositive()) RateLimiter.inf
    else 1.0 / interval.toDouble(DurationUnit.SECONDS)