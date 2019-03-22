package company.evo.persistent

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class RefCountedTests : StringSpec() {
    init {
        "atomic reference counted" {
            var v = 0
            val rc = AtomicRefCounted(100) {
                v = it
            }

            rc.refCount() shouldBe 1

            rc.acquire() shouldBe 100
            rc.refCount() shouldBe 2

            rc.use {
                it shouldBe 100
                rc.refCount() shouldBe 3
            }
            rc.refCount() shouldBe 2

            rc.release()
            rc.refCount() shouldBe 1

            v shouldBe 0
            rc.release()
            v shouldBe 100

            shouldThrow<IllegalStateException> {
                rc.acquire()
            }
            shouldThrow<IllegalStateException> {
                rc.release()
            }
        }
    }
}
