package company.evo.persistent

import java.util.concurrent.atomic.AtomicLong

interface RefCounted<T> {
    fun count(): Long
    fun acquire(): T
    fun release()
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

    override fun acquire(): T {
        ensureAlive()
        count.incrementAndGet()
        return value
    }

    override fun release() {
        ensureAlive()
        if (count.decrementAndGet() == 0L) {
            drop(value)
        }
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
