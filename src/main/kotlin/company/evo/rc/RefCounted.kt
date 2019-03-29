package company.evo.rc

import java.lang.ref.Cleaner
import java.lang.reflect.ParameterizedType
import java.util.concurrent.atomic.AtomicLong

class IllegalRefCountException : Exception()

interface RefCounted<out T> {
    fun refCount(): Long
    fun get(): T
    fun retain(): T?
    fun release(): Boolean
}

class AtomicRefCounted<out T>(
        private val value: T,
        private val drop: (v: T) -> Unit
) : RefCounted<T> {

    companion object {
        private val cleaner = Cleaner.create()

        private fun isValid(rc: Long) = rc and 1 == 0L

        private fun ensureValid(rc: Long) {
            if (!isValid(rc)) {
                throw IllegalRefCountException()
            }
        }
    }

    // even - value is alive, odd - value was dropped
    private val rc = AtomicLong(2)

    class State(private val rc: AtomicLong, private val desc: String) : Runnable {
        override fun run() {
            val count = rc.get()
            if (isValid(count)) {
                println("WARNING! Reference counted object <$desc> was not dropped: " +
                        "number of references is ${rc.get() ushr 1}")
            }
        }
    }

    init {
        cleaner.register(this, State(rc, value.toString()))
    }

    override fun refCount(): Long {
        val rc = rc.get()
        ensureValid(rc)
        return rc ushr 1
    }

    override fun get(): T {
        ensureValid(rc.get())
        return value
    }

    override fun retain(): T? {
        val oldRc = rc.getAndAdd(2)
        if (!isValid(oldRc)) {
            return null
        }
        return value
    }

    override fun release(): Boolean {
        while (true) {
            val curRc = rc.get()
            ensureValid(curRc)
            return if (curRc == 2L) {
                if (!rc.compareAndSet(curRc, 1)) {
                    continue
                }
                drop(value)
                true
            } else {
                if (!rc.compareAndSet(curRc, curRc - 2)) {
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
