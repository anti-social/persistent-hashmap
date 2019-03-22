package company.evo.persistent

import java.util.concurrent.atomic.AtomicLong

interface RefCounted<T> {
    fun count(): Long
    fun get(): T
    fun acquire(): T
    fun release(): Boolean
}

class AtomicRefCounted<T>(
        private val value: T,
        private val drop: (v: T) -> Unit
) : RefCounted<T> {
    private val count = AtomicLong(1)

    override fun count(): Long {
        ensureAlive()
        return count.get()
    }

    override fun get(): T {
        ensureAlive()
        return value
    }

    override fun acquire(): T {
        ensureAlive()
        count.incrementAndGet()
        return value
    }

    override fun release(): Boolean {
        ensureAlive()
        if (count.decrementAndGet() == 0L) {
            drop(value)
            return true
        }
        return false
    }

    private fun ensureAlive() {
        if (count.get() == 0L) {
            throw IllegalStateException("Have been already dropped")
        }
    }
}

fun <T, R> RefCounted<T>.use(block: (T) -> R): R {
    try {
        return block(acquire())
    } finally {
        release()
    }
}
