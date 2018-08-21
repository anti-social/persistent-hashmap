package company.evo.persistent

import company.evo.persistent.util.withTempDir
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class VersionedDirectoryTest : StringSpec() {
    companion object {
        const val VERSION_FILENAME = "test.ver"
    }

    init {
        "one writer, several readers" {
            withTempDir {
                VersionedDirectory.openWritable(it, VERSION_FILENAME).use { dir ->
                    dir.readVersion() shouldBe 0L

                    shouldThrow<WriteLockException> {
                        VersionedDirectory.openWritable(it, VERSION_FILENAME)
                    }

                    val dirRO = VersionedDirectory.openReadOnly(it, VERSION_FILENAME)
                    dirRO.readVersion() shouldBe 0L

                    dir.writeVersion(1L)
                    dirRO.readVersion() shouldBe 1L
                }

                VersionedDirectory.openWritable(it, VERSION_FILENAME).use { dir ->
                    dir.readVersion() shouldBe 1L
                }
            }
        }

        "read uninitialized directory" {
            withTempDir {
                shouldThrow<FileDoesNotExistException> {
                    VersionedDirectory.openReadOnly(it, VERSION_FILENAME)
                }
            }
        }
    }
}