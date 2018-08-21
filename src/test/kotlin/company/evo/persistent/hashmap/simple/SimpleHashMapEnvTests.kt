package company.evo.persistent.hashmap.simple

import company.evo.persistent.VersionedDirectoryException
import company.evo.persistent.util.withTempDir

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.FunSpec

class SimpleHashMapEnvTests : FunSpec() {
    init {
        test("test") {
            1 shouldBe 1
        }

        test("env: single writer, multiple readers") {
            withTempDir { tmpDir ->
                SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                        .open(tmpDir)
                        .use { env ->
                            env.getCurrentVersion() shouldBe 0L

                            SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                                    .openReadOnly(tmpDir)
                                    .use { roEnv ->
                                        roEnv.getCurrentVersion() shouldBe 0L
                                    }

                            shouldThrow<VersionedDirectoryException> {
                                SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                                        .open(tmpDir)
                            }
                        }

                SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                        .open(tmpDir)
                        .use { env ->
                            env.getCurrentVersion() shouldBe 0L
                        }
            }
        }

        test("env: copy map") {
            withTempDir { tmpDir ->
                SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                        .open(tmpDir)
                        .use { env ->
                            val map = env.getMap()
                            env.getCurrentVersion() shouldBe 0L
                            env.copyMap(map)
                            env.getCurrentVersion() shouldBe 1L

                            SimpleHashMapEnv.Builder(Int::class.javaObjectType, Float::class.javaObjectType)
                                    .openReadOnly(tmpDir)
                                    .use { roEnv ->
                                        roEnv.getCurrentVersion() shouldBe 1L
                                    }
                        }
            }
        }
    }
}
