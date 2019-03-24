package company.evo.persistent

import java.util.concurrent.atomic.AtomicLong

class IllegalRefCountException() : Exception()

interface RefCounted<T> {
    fun refCount(): Long
    fun get(): T
    fun retain(): T?
    fun release(): Boolean
}

class AtomicRefCounted<T>(
        private val value: T,
        private val drop: (v: T) -> Unit
) : RefCounted<T> {

    // even - value is alive, odd - value was dropped
    private val rc = AtomicLong(2)

    override fun refCount(): Long {
        val rc = rc.get()
        if (rc and 1 != 0L) {
            throw IllegalRefCountException()
        }
        return rc ushr 1
    }

    override fun get(): T {
        val rc = rc.get()
        if (rc and 1 != 0L) {
            throw IllegalRefCountException()
        }
        return value
    }

    override fun retain(): T? {
        val oldRc = rc.getAndAdd(2)
        if (oldRc and 1 != 0L) {
            return null
        }
        return value
    }

    override fun release(): Boolean {
        while (true) {
            val oldRc = rc.get()
            return if (oldRc == 2L) {
                if (!rc.compareAndSet(oldRc, 1)) {
                    continue
                }
                drop(value)
                true
            } else {
                if (!rc.compareAndSet(oldRc, oldRc - 2)) {
                    continue
                }
                false
            }
        }
    }
}

inline fun <T, R> RefCounted<T>.use(block: (T) -> R): R {
    var acquired = false
    try {
        val value = retain()
        if (value != null) {
            acquired = true
            return block(value)
        }
        throw IllegalRefCountException()
    } finally {
        if (acquired) {
            release()
        }
    }
}
