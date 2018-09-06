package company.evo.persistent.rc

import java.util.concurrent.atomic.AtomicLong

class RefCounted<out T> private constructor(
        private val obj: T,
        private val counter: AtomicLong
): Cloneable, AutoCloseable
        where T: AutoCloseable, T: Cloneable {
    constructor(obj: T) : this(obj, AtomicLong(1L))

    public override fun clone(): RefCounted<T> {
        if (counter.get() <= 0) {
            throw RuntimeException("Reference counted object is closed")
        }
        counter.incrementAndGet()
        return RefCounted(obj, counter)
    }

    override fun close() {
        if (counter.decrementAndGet() == 0L) {
            obj.close()
        }
    }

    fun get(): T = obj
}
