package company.evo.persistent

import java.util.concurrent.atomic.AtomicLong

class IllegalRefCountException() : Exception()

interface RefCounted<T> {
    fun refCount(): Long
    fun get(): T
    fun acquire(): T
    fun release(): Boolean
}

class AtomicRefCounted<T>(
        private val value: T,
        private val drop: (v: T) -> Unit
) : RefCounted<T> {
    private val rc = AtomicLong(1)

    override fun refCount(): Long {
        ensureAlive()
        return rc.get()
    }

    override fun get(): T {
        ensureAlive()
        return value
    }

    override fun acquire(): T {
        val oldRc = rc.getAndIncrement()
        if (oldRc == 0L) {
            
        }
        return value
    }

    override fun release(): Boolean {
        ensureAlive()
        if (rc.decrementAndGet() == 0L) {
            drop(value)
            return true
        }
        return false
    }

    private fun ensureAlive() {
        if (rc.get() == 0L) {
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
