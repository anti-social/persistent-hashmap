package company.evo.persistent.rc

import io.kotlintest.matchers.beTheSameInstanceAs
import io.kotlintest.should
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class RefCountedTests : StringSpec() {
    class Obj : AutoCloseable {
        override fun close() {
            TODO("not implemented")
        }
    }

    init {
        "close called" {
            val obj = Obj()
            val rc = RefCounted(obj)
            rc.clone().use {
                it.get() should beTheSameInstanceAs(obj)
            }
            shouldThrow<NotImplementedError> {
                rc.close()
            }
        }
    }
}