package dev.evo.rc

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe


class AtomicRefCountedTests : StringSpec() {
    init {
        "atomic reference counted" {
            var v = 0
            val rc = AtomicRefCounted(100) {
                v = it
            }

            rc.refCount() shouldBe 1

            rc.retain() shouldBe 100
            rc.refCount() shouldBe 2

            rc.use {
                it shouldBe 100
                rc.refCount() shouldBe 3
            }
            rc.refCount() shouldBe 2

            rc.release() shouldBe false
            rc.refCount() shouldBe 1

            v shouldBe 0
            rc.release() shouldBe true
            v shouldBe 100

            rc.retain() shouldBe null
            shouldThrow<IllegalRefCountException> {
                rc.release()
            }
            shouldThrow<IllegalRefCountException> {
                rc.get()
            }
            shouldThrow<IllegalRefCountException> {
                rc.refCount()
            }
        }
    }
}
