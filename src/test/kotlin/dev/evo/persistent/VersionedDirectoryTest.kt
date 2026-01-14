package dev.evo.persistent

import dev.evo.persistent.util.withTempDir

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class VersionedDirectoryTest : StringSpec() {
    companion object {
        const val VERSION_FILENAME = "test.ver"
    }

    init {
        "one writer, several readers" {
            withTempDir {
                VersionedMmapDirectory.openWritable(it, VERSION_FILENAME, BufferManagement.MemorySegments).use { dir ->
                    dir.readVersion() shouldBe 0L

                    shouldThrow<WriteLockException> {
                        VersionedMmapDirectory.openWritable(it, VERSION_FILENAME, BufferManagement.MemorySegments)
                    }

                    val dirRO = VersionedMmapDirectory.openReadOnly(it, VERSION_FILENAME, BufferManagement.MemorySegments)
                    dirRO.readVersion() shouldBe 0L

                    dir.writeVersion(1L)
                    dirRO.readVersion() shouldBe 1L
                }

                VersionedMmapDirectory.openWritable(it, VERSION_FILENAME, BufferManagement.MemorySegments).use { dir ->
                    dir.readVersion() shouldBe 1L
                }
            }
        }

        "read uninitialized directory" {
            withTempDir {
                shouldThrow<FileDoesNotExistException> {
                    VersionedMmapDirectory.openReadOnly(it, VERSION_FILENAME, BufferManagement.MemorySegments)
                }
            }
        }

        "RAM heap directory" {
            VersionedRamDirectory.createHeap().use { dir ->
                dir.readVersion() shouldBe 0L

                dir.writeVersion(1L)
                dir.readVersion() shouldBe 1L
            }
        }

        "RAM direct directory" {
            VersionedRamDirectory.createDirect(BufferManagement.MemorySegments).use { dir ->
                dir.readVersion() shouldBe 0L

                dir.writeVersion(1L)
                dir.readVersion() shouldBe 1L
            }
        }
    }
}
