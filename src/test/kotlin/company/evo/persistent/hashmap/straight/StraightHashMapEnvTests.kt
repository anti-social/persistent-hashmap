package company.evo.persistent.hashmap.straight

import company.evo.persistent.VersionedDirectoryException
import company.evo.persistent.util.withTempDir

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.FunSpec

class StraightHashMapEnvTests : FunSpec() {
    init {
        test("env: single writer, multiple readers") {
            withTempDir { tmpDir ->
                StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                        .useUnmapHack(true)
                        .open(tmpDir)
                        .use { env ->
                            env.getCurrentVersion() shouldBe 0L

                            StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                                    .openReadOnly(tmpDir)
                                    .use { roEnv ->
                                        roEnv.getCurrentVersion() shouldBe 0L
                                    }

                            shouldThrow<VersionedDirectoryException> {
                                StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                                        .open(tmpDir)
                            }
                        }

                StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                        .open(tmpDir)
                        .use { env ->
                            env.getCurrentVersion() shouldBe 0L
                        }
            }
        }

        test("env: copy map") {
            withTempDir { tmpDir ->
                StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                        .useUnmapHack(true)
                        .open(tmpDir)
                        .use { env ->
                            env.openMap().use { map ->
                                map.put(1, 1.1F) shouldBe PutResult.OK
                                map.put(2, 1.2F) shouldBe PutResult.OK
                                env.getCurrentVersion() shouldBe 0L

                                StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                                        .openReadOnly(tmpDir)
                                        .use { roEnv ->
                                            roEnv.getCurrentVersion() shouldBe 0L
                                            val mapV0 = roEnv.getCurrentMap()

                                            env.copyMap(map).use { newMap ->
                                                env.getCurrentVersion() shouldBe 1L
                                                newMap.put(3, 1.3F) shouldBe PutResult.OK
                                            }

                                            roEnv.getCurrentVersion() shouldBe 1L
                                            val mapV1 = roEnv.getCurrentMap()

                                            mapV0.version shouldBe 0L
                                            mapV0.maxEntries shouldBe 1024
                                            mapV0.capacity shouldBe 1597
                                            mapV0.get(1, 0.0F) shouldBe 1.1F
                                            mapV0.get(2, 0.0F) shouldBe 1.2F
                                            mapV0.get(3, 0.0F) shouldBe 0.0F

                                            mapV1.version shouldBe 1L
                                            mapV1.maxEntries shouldBe 4
                                            mapV1.capacity shouldBe 7
                                            mapV1.get(1, 0.0F) shouldBe 1.1F
                                            mapV1.get(2, 0.0F) shouldBe 1.2F
                                            mapV1.get(3, 0.0F) shouldBe 1.3F

                                            mapV0.close()
                                            mapV1.close()
                                        }
                            }
                        }
            }
        }

        test("env: anonymous") {
            StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                    .createAnonymousHeap()
                    .use { env ->
                        env.openMap()
                    }
        }
    }
}
