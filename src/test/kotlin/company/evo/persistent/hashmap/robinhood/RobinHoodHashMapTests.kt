package company.evo.persistent.hashmap.robinhood

import company.evo.io.MutableUnsafeBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.Dummy32
import company.evo.persistent.hashmap.PutResult
import company.evo.rc.AtomicRefCounted
import io.kotlintest.properties.Gen

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

import java.nio.ByteBuffer
import java.util.*

class RobinHoodHashMapTests : StringSpec() {
    init {
        "single put" {
            val map = createMap_Int_Float(7)
            map.put(1, 1.0F)
            map.get(1, 0.0F) shouldBe 1.0F
            map.get(12, 0.0F) shouldBe 0.0F
        }

        "put with collision" {
            val map = createMap_Int_Float(7)
            map.put(1, 1.0F)
            map.put(12, 12.0F)
            map.get(1, 0.0F) shouldBe 1.0F
            map.get(12, 0.0F) shouldBe 12.0F
            map.get(0, 0.0F) shouldBe 0.0F
        }

        "put with collision then remove" {
            val map = createMap_Int_Float(7)
            map.put(1, 1.0F)
            map.put(12, 12.0F)
            map.remove(1)
            map.get(1, 0.0F) shouldBe 0.0F
            map.get(12, 0.0F) shouldBe 12.0F
            map.get(0, 0.0F) shouldBe 0.0F
        }

        "put with collision then remove all" {
            val map = createMap_Int_Float(7)
            map.put(1, 1.0F)
            map.put(12, 12.0F)
            map.remove(1)
            map.remove(12)
            map.get(1, 0.0F) shouldBe 0.0F
            map.get(12, 0.0F) shouldBe 0.0F
            map.get(0, 0.0F) shouldBe 0.0F
        }

        "put with collision and shifting" {
            val map = createMap_Int_Float(7)
            map.put(1, 1.0F)
            map.put(12, 12.0F)
            map.put(23, 23.0F)
            map.put(0, Float.NaN)
            map.put(11, 11.0F)
            map.put(10, 10.0F)
            map.put(21, 21.0F)
            map.get(1, 0.0F) shouldBe 1.0F
            map.get(12, 0.0F) shouldBe 12.0F
            map.get(23, 0.0F) shouldBe 23.0F
            map.get(0, 0.0F) shouldBe Float.NaN
            map.get(11, 0.0F) shouldBe 11.0F
        }

        "int double: 12".config(invocations = 10_000) {
            testRandomPutRemove_Int_Float(12, 17, true)
        }

        "int double: 20".config(invocations = 10_000) {
            testRandomPutRemove_Int_Float(20, 29, true)
        }

        "int double: 100".config(invocations = 1000) {
            testRandomPutRemove_Int_Float(100, 163, true)
        }

        "int double: 1000".config(invocations = 100) {
            testRandomPutRemove_Int_Float(1000, 1597)
        }

        "int double: one in a million".config(invocations = 1) {
            testRandomPutRemove_Int_Float(1_000_000, 1395263)
        }

        "fixed" {
            val map = createMap_Int_Float(20)
            map.put(5, 0.11770457F) shouldBe PutResult.OK
            map.put(3, 0.9881872F) shouldBe PutResult.OK
            map.put(409, 0.27982473F) shouldBe PutResult.OK
            map.put(351, 0.80483353F) shouldBe PutResult.OK
            map.put(9, 0.18219483F) shouldBe PutResult.OK
            map.put(148, 0.58224F) shouldBe PutResult.OK
            map.put(7, 0.84601575F) shouldBe PutResult.OK
            map.remove(7) shouldBe true
            map.put(4, 0.021135628F) shouldBe PutResult.OK
            map.put(554, 0.2589075F) shouldBe PutResult.OK
            map.put(293, 0.22883373F) shouldBe PutResult.OK
            map.remove(293) shouldBe true
            map.put(8, 0.9225263F) shouldBe PutResult.OK
            map.put(903, 0.7062499F) shouldBe PutResult.OK
            map.put(4, 0.47684807F) shouldBe PutResult.OK
            map.size() shouldBe 10
        }

        "fixed 2" {
            // Generate data
            val map = createMap_Int_Float(20)
            map.put(1, 0.7814683F) shouldBe PutResult.OK
            map.size() shouldBe 1
            map.put(3, 0.6459955F) shouldBe PutResult.OK
            map.size() shouldBe 2
            map.put(7, 0.9104234F) shouldBe PutResult.OK
            map.size() shouldBe 3
            map.put(9, 0.6892022F) shouldBe PutResult.OK
            map.size() shouldBe 4
            map.put(0, 0.09816152F) shouldBe PutResult.OK
            map.size() shouldBe 5
            map.put(2, 0.2270844F) shouldBe PutResult.OK
            map.size() shouldBe 6
            map.put(4, 0.042714536F) shouldBe PutResult.OK
            map.size() shouldBe 7
            map.put(1046, 0.24858886F) shouldBe PutResult.OK
            map.size() shouldBe 8
            map.put(5, 0.92489934F) shouldBe PutResult.OK
            map.size() shouldBe 9
            map.put(1191, 0.1219666F) shouldBe PutResult.OK
            map.size() shouldBe 10
            map.remove(1191) shouldBe true
            map.size() shouldBe 9
            map.remove(2) shouldBe true
            map.size() shouldBe 8
            map.put(1191, 0.6127803F) shouldBe PutResult.OK
            map.size() shouldBe 9
        }

        "fixed 3" {
            // Generate data
            val map = createMap_Int_Float(20)
            map.put(7, 0.046260893F) shouldBe PutResult.OK
            map.put(3, 0.19764632F) shouldBe PutResult.OK
            map.put(9, 0.14726537F) shouldBe PutResult.OK
            map.put(0, 0.7643558F) shouldBe PutResult.OK
            map.remove(7) shouldBe true
            map.put(32, 0.84833467F) shouldBe PutResult.OK
            map.put(5, 0.97258854F) shouldBe PutResult.OK
            map.put(583, 0.9245855F) shouldBe PutResult.OK
            map.put(4, 0.7320889F) shouldBe PutResult.OK
            map.remove(4) shouldBe true
            map.remove(3) shouldBe true
            map.put(4, 0.5604667F) shouldBe PutResult.OK
            map.put(235, 0.9832549F) shouldBe PutResult.OK
            map.put(3, 0.6416215F) shouldBe PutResult.OK
            map.put(0, 0.5953873F) shouldBe PutResult.OK
            map.put(4, 0.8113354F) shouldBe PutResult.OK
            map.remove(4) shouldBe true
            // Assertions
            map.get(3, 1.4E-45F) shouldBe 0.6416215F
        }
    }

    private val seed = System.getProperty("test.random.seed")?.toLong() ?: Random().nextLong()
    private val random = Random(seed)

    private fun testRandomPutRemove_Int_Float(
        limit: Int, expectedCapacity: Int,
        generateTestCaseCode: Boolean = false
    ) {
        val entries = hashMapOf<Int, Float>()

        val generatedTestCase = if (generateTestCaseCode) StringBuilder() else null
        generatedTestCase?.append("// Generate data\n")
        generatedTestCase?.append("val map = createMap_Int_Float($limit)\n")
        try {
            createMap_Int_Float(limit).use { map ->
                map.maxEntries shouldBe limit
                map.capacity shouldBe expectedCapacity

                val keysGen = object : Gen<Int> {
                    val keysStream = random.ints(0, limit / 2)
                    var collisionCandidate = Int.MIN_VALUE
                    var removeCandidate = Int.MIN_VALUE

                    override fun constants(): Iterable<Int> = emptyList()

                    override fun random(): Sequence<Int> = sequence {
                        for (v in keysStream) {
                            removeCandidate = when {
                                random.nextInt(3) == 0 -> {
                                    // Generate collisions
                                    val k = random.nextInt(limit * 10)
                                    val ix = collisionCandidate % expectedCapacity
                                    val collidedValue = v * k / expectedCapacity * expectedCapacity + ix
                                    yield(collidedValue)
                                    collidedValue

                                }
                                random.nextInt(4) == 0 -> {
                                    yield(-removeCandidate)
                                    collisionCandidate
                                }
                                else -> {
                                    yield(v)
                                    v
                                }
                            }
                            if (random.nextInt(10) == 0 || collisionCandidate == Int.MIN_VALUE) {
                                collisionCandidate = v
                            }
                        }
                    }
                }
                keysGen.random().take(limit).forEach { k ->
                    if (k < 0) {
                        val removeKey = -k
                        val removeRes = entries.remove(removeKey) != null
                        generatedTestCase?.append("map.remove($removeKey) shouldBe $removeRes\n")
                        map.remove(removeKey) shouldBe removeRes
                        generatedTestCase?.append("map.get($removeKey, 0.0F) shouldBe 0.0F\n")
                        map.get(removeKey, 0.0F) shouldBe 0.0F
                    } else {
                        val v = random.nextFloat()
                        entries[k] = v
                        generatedTestCase?.append("map.put($k, ${v}F) shouldBe PutResult.OK\n")
                        map.put(k, v) shouldBe PutResult.OK
                        generatedTestCase?.append("map.get($k, 0.0F) shouldBe ${v}F\n")
                        map.get(k, 0.0F) shouldBe v
                    }
                    generatedTestCase?.append("map.size() shouldBe ${entries.size}\n")
                    map.size() shouldBe entries.size
                }

                generatedTestCase?.append("// Assertions\n")
                // println(map.dump(true))
                (0..limit).forEach { k ->
                    val expectedValue = entries[k]
                    val v = map.get(k, Float.MIN_VALUE)
                    generatedTestCase?.append("map.get($k, ${Float.MIN_VALUE}F) shouldBe ${expectedValue ?: Float.MIN_VALUE}F\n")
                    v shouldBe (expectedValue ?: Float.MIN_VALUE)
                }
            }
        } catch (e: AssertionError) {
            if (generatedTestCase != null) {
                println(generatedTestCase)
                println()
                for ((k, v) in entries) {
                    println("entries[$k] == $v")
                }
            }
            throw e
        }
    }

    companion object {
        private fun createMap_Int_Float(
            maxEntries: Int, loadFactor: Double = 0.75
        ): RobinHoodHashMap_Int_Float {
            val mapInfo = MapInfo.calcFor(
                maxEntries, loadFactor, RobinHoodHashMapType_Int_Float.bucketLayout.size
            )
            val buffer = ByteBuffer.allocate(mapInfo.bufferSize)
            mapInfo.initBuffer(
                MutableUnsafeBuffer(buffer),
                RobinHoodHashMapType_Int_Float.keySerializer,
                RobinHoodHashMapType_Int_Float.valueSerializer,
                RobinHoodHashMapType_Int_Float.hasherProvider.getHasher(Dummy32.serial)
            )
            val file = AtomicRefCounted(
                MappedFile("<map>", MutableUnsafeBuffer(buffer))
            ) {}
            return RobinHoodHashMap_Int_Float(0L, file)
        }
    }
}