package net.cutereimu.kotlinx.coroutines

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicReference

class ConcurrentHashMap<K : Any, V : Any> {
    private val mu = Mutex()
    private val read = AtomicReference<ReadOnly<K>>()
    private var dirty: HashMap<K, Entry>? = null
    private var misses = 0

    val size: Int = readOnly.m.size
    fun isEmpty(): Boolean = size == 0
    fun isNotEmpty(): Boolean = size != 0

    suspend fun clear() {
        mu.withLock {
            read.set(null)
            dirty = null
            misses = 0
        }
    }

    @Suppress("UNCHECKED_CAST")
    suspend fun remove(key: K): V? {
        var read = readOnly
        var e = readOnly.m[key]
        if (e == null && read.amended) {
            mu.withLock {
                read = readOnly
                e = read.m[key]
                if (e == null && read.amended) {
                    e = dirty!![key]
                    dirty!!.remove(key)
                    missLocked()
                }
            }
        }
        return e?.remove() as V?
    }

    suspend fun putAll(from: Map<out K, V>) = from.forEach { (key, value) -> put(key, value) }
    suspend fun putAll(from: ConcurrentHashMap<out K, V>) = from.forEach { (key, value) -> put(key, value) }

    @Suppress("UNCHECKED_CAST")
    suspend fun get(key: K): V? {
        var read = readOnly
        var e = read.m[key]
        if (e == null && read.amended) {
            mu.withLock {
                read = readOnly
                e = read.m[key]
                if (e == null && read.amended) {
                    e = dirty!![key]
                    missLocked()
                }
            }
        }
        return e?.get() as V?
    }

    @Suppress("UNCHECKED_CAST")
    suspend fun put(key: K, value: V): V? {
        readOnly.m[key]?.let { e -> return e.trySwap(value) as V? }

        mu.withLock {
            val read = readOnly
            read.m[key]?.let { e ->
                if (e.unexpungeLocked())
                    dirty!![key] = e
                e.swapLocked(value)?.let { v -> return v as V? }
                return null
            }
            dirty?.get(key)?.let { e ->
                e.swapLocked(value)?.let { v -> return v as V? }
                return null
            }
            if (!read.amended) {
                dirtyLocked()
                this.read.set(ReadOnly(m = read.m, amended = true))
            }
            dirty!![key] = Entry(value)
            return null
        }
    }

    @Suppress("UNCHECKED_CAST")
    suspend fun getOrPut(key: K, defaultValue: () -> V): V {
        readOnly.m[key]?.let { e ->
            return e.tryGetOrPut(defaultValue()) as V? ?: return@let
        }

        mu.withLock {
            val read = readOnly
            read.m[key]?.let { e ->
                if (e.unexpungeLocked()) dirty!![key] = e
                return e.tryGetOrPut(defaultValue()) as V
            }
            dirty?.get(key)?.let { e ->
                val actual = e.tryGetOrPut(defaultValue()) as V
                missLocked()
                return actual
            }
            if (!read.amended) {
                dirtyLocked()
                this.read.set(ReadOnly(m = read.m, amended = true))
            }
            val value = defaultValue()
            dirty!![key] = Entry(value)
            return value
        }
    }


    suspend fun containsKey(key: K): Boolean = get(key) != null

    @Suppress("UNCHECKED_CAST")
    suspend fun forEach(action: suspend (Map.Entry<K, V>) -> Unit) {
        var read = readOnly
        if (read.amended) {
            mu.withLock {
                read = readOnly
                if (read.amended) {
                    read = ReadOnly(m = dirty!!)
                    this.read.set(read)
                    dirty = null
                    misses = 0
                }
            }
        }

        for ((k, e) in read.m) {
            val v = e.get() as V? ?: continue
            action(object : Map.Entry<K, V> {
                override val key: K = k
                override val value: V = v
            })
        }
    }

    private fun missLocked() {
        misses++
        if (misses < dirty!!.size) return
        read.set(ReadOnly(m = dirty!!))
        dirty = null
        misses = 0
    }

    private fun dirtyLocked() {
        if (dirty != null) return
        val read = readOnly
        dirty = HashMap(read.m.size)
        read.m.forEach { (k, e) ->
            if (!e.tryExpungeLocked())
                dirty!![k] = e
        }
    }

    private val readOnly: ReadOnly<K>
        get() = read.get() ?: ReadOnly()

    private class ReadOnly<K>(
        val m: Map<K, Entry> = emptyMap(),
        val amended: Boolean = false
    )

    private class Entry(i: Any) {
        val p = AtomicReference<Any>()

        init {
            p.set(i)
        }

        fun get(): Any? = p.get()

        fun trySwap(i: Any?): Any? {
            while (true) {
                val p = this.p.get() ?: return null
                if (this.p.compareAndSet(p, i)) return p
            }
        }

        fun unexpungeLocked(): Boolean = p.compareAndSet(expunged, null)

        fun swapLocked(i: Any?): Any? = p.getAndSet(i)

        fun tryExpungeLocked(): Boolean {
            var p = this.p.get()
            while (p == null) {
                if (this.p.compareAndSet(null, expunged)) return true
                p = this.p.get()
            }
            return p === expunged
        }

        fun tryGetOrPut(i: Any?): Any? {
            var p = this.p.get()
            if (p === expunged) return null
            if (p != null) return p
            while (true) {
                if (this.p.compareAndSet(null, i)) return i
                p = this.p.get()
                if (p === expunged) return null
                if (p != null) return p
            }
        }

        fun remove(): Any? {
            while (true) {
                val p = this.p.get()
                if (p == null || p == expunged) return null
                if (this.p.compareAndSet(p, null)) return p
            }
        }
    }

    companion object {
        val expunged = Any()
    }
}