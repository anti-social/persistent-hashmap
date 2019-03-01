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
                SimpleHashMapEnv_K_V.Builder()
                        .open(tmpDir)
                        .use { env ->
                            env.getCurrentVersion() shouldBe 0L

                            SimpleHashMapEnv_K_V.Builder()
                                    .openReadOnly(tmpDir)
                                    .use { roEnv ->
                                        roEnv.getCurrentVersion() shouldBe 0L
                                    }

                            shouldThrow<VersionedDirectoryException> {
                                SimpleHashMapEnv_K_V.Builder()
                                        .open(tmpDir)
                            }
                        }

                SimpleHashMapEnv_K_V.Builder()
                        .open(tmpDir)
                        .use { env ->
                            env.getCurrentVersion() shouldBe 0L
                        }
            }
        }

        test("env: copy map") {
            withTempDir { tmpDir ->
                SimpleHashMapEnv_K_V.Builder()
                        .open(tmpDir)
                        .use { env ->
                            val map = env.openMap()
                            env.getCurrentVersion() shouldBe 0L
                            env.copyMap(map)
                            env.getCurrentVersion() shouldBe 1L

                            SimpleHashMapEnv_K_V.Builder()
                                    .openReadOnly(tmpDir)
                                    .use { roEnv ->
                                        roEnv.getCurrentVersion() shouldBe 1L
                                    }
                        }
            }
        }

        test("env: anonymous") {
            SimpleHashMapEnv_K_V.Builder()
                    .createAnonymousHeap()
                    .use { env ->
                        env.openMap()
                    }
        }
    }
}
