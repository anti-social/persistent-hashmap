package company.evo.persistent.hashmap.robinhood

import company.evo.io.MutableUnsafeBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.Dummy32
import company.evo.persistent.hashmap.Hash32
import company.evo.rc.AtomicRefCounted
import io.kotlintest.properties.Gen

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

import java.nio.ByteBuffer
import java.util.*

class RobinHoodHashMapTests : StringSpec() {
    init {
        // "single put" {
        //     val map = createMap_Int_Float(7)
        //     map.put(1, 1.1F)
        //     map.get(1, 0.0F) shouldBe 1.0F
        //     map.get(12, 0.0F) shouldBe 0.0F
        // }
        //
        // "put with collision" {
        //     val map = createMap_Int_Float(7)
        //     map.put(1, 1.0F)
        //     map.put(12, 12.0F)
        //     println(map.dump(true))
        //     map.get(1, 0.0F) shouldBe 1.0F
        //     map.get(12, 0.0F) shouldBe 12.0F
        //     map.get(0, 0.0F) shouldBe 0.0F
        // }
        //
        // "put with collision then remove" {
        //     val map = createMap_Int_Float(7)
        //     map.put(1, 1.0F)
        //     map.put(12, 12.0F)
        //     map.remove(1)
        //     println(map.dump(true))
        //     map.get(1, 0.0F) shouldBe 0.0F
        //     map.get(12, 0.0F) shouldBe 12.0F
        //     map.get(0, 0.0F) shouldBe 0.0F
        // }
        //
        // "put with collision then remove all" {
        //     val map = createMap_Int_Float(7)
        //     map.put(1, 1.0F)
        //     map.put(12, 12.0F)
        //     map.remove(1)
        //     map.remove(12)
        //     println(map.dump(true))
        //     map.get(1, 0.0F) shouldBe 0.0F
        //     map.get(12, 0.0F) shouldBe 0.0F
        //     map.get(0, 0.0F) shouldBe 0.0F
        // }

        // "put with collision and shifting" {
        //     val map = createMap_Int_Float(7)
        //     map.put(1, 1.0F)
        //     map.put(12, 12.0F)
        //     map.put(23, 23.0F)
        //     map.put(0, Float.NaN)
        //     map.put(11, 11.0F)
        //     map.put(10, 10.0F)
        //     map.put(21, 21.0F)
        //     println(map.dump(true))
        //     map.get(1, 0.0F) shouldBe 1.0F
        //     map.get(12, 0.0F) shouldBe 12.0F
        //     map.get(23, 0.0F) shouldBe 23.0F
        //     map.get(0, 0.0F) shouldBe Float.NaN
        //     map.get(11, 0.0F) shouldBe 11.0F
        // }

        // "int double".config(invocations = 100) {
        //     testRandomPutRemove_Int_Float(12, 17)
        // }

        // "int double: 100".config(invocations = 100) {
        //     testRandomPutRemove_Int_Float(20, 29, generateTestCaseCode = true)
        // }

        "failed" {
            // Generate data
            val map = createMap_Int_Float(20)
            map.put(5, 0.11770457F) shouldBe PutResult.OK
            map.put(3, 0.9881872F) shouldBe PutResult.OK
            map.put(409, 0.27982473F) shouldBe PutResult.OK
            map.put(351, 0.80483353F) shouldBe PutResult.OK
            map.put(9, 0.18219483F) shouldBe PutResult.OK
            // map.put(5, 0.26596248F) shouldBe PutResult.OK
            map.put(148, 0.58224F) shouldBe PutResult.OK
            map.put(7, 0.84601575F) shouldBe PutResult.OK
            map.remove(7) shouldBe true
            map.put(4, 0.021135628F) shouldBe PutResult.OK
            map.put(554, 0.2589075F) shouldBe PutResult.OK
            map.put(293, 0.22883373F) shouldBe PutResult.OK
            map.remove(293) shouldBe true
            println(map.dump(true))
            map.put(8, 0.9225263F) shouldBe PutResult.OK
            map.put(903, 0.7062499F) shouldBe PutResult.OK
            map.put(4, 0.47684807F) shouldBe PutResult.OK
            // map.put(121, 0.41394937F) shouldBe PutResult.OK
            // map.put(382, 0.7238621F) shouldBe PutResult.OK
            // map.put(8, 0.7997527F) shouldBe PutResult.OK
            // map.put(95, 0.08528036F) shouldBe PutResult.OK
            println(map.dump(true))
            // Assertions
            map.size() shouldBe 10
        }
    }

    private val seed = System.getProperty("test.random.seed")?.toLong() ?: Random().nextLong()
    private val random = Random(seed)

    private fun testRandomPutRemove_Int_Float(
        limit: Int, expectedCapacity: Int,
        generateTestCaseCode: Boolean = false
    ) {
        val entries = hashMapOf<Int, Float>()

        if (generateTestCaseCode) {
            println("// Generate data")
            println("val map = createMap_Int_Float<Int, Float>($limit)")
        }
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
                    if (generateTestCaseCode) {
                        println("map.remove($removeKey) shouldBe $removeRes")
                    }
                    map.remove(removeKey) shouldBe removeRes
                } else {
                    val v = random.nextFloat()
                    entries.put(k, v)
                    if (generateTestCaseCode) {
                        println("map.put($k, ${v}F) shouldBe PutResult.OK")
                    }
                    map.put(k, v) shouldBe PutResult.OK
                }
            }

            if (generateTestCaseCode) {
                println("// Assertions")
            }

            if (generateTestCaseCode) {
                println("map.size() shouldBe ${entries.size}")
            }
            map.size() shouldBe entries.size
            // println(map.dump(true))
            (0..limit).forEach { k ->
                val expectedValue = entries[k]
                val v = map.get(k, Float.MIN_VALUE)
                if (generateTestCaseCode) {
                    println("map.get($k, ${Float.MIN_VALUE}F) shouldBe ${expectedValue ?: Float.MIN_VALUE}F")
                }
                v shouldBe (expectedValue ?: Float.MIN_VALUE)
            }
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
            return RobinHoodHashMapImpl_Int_Float(0L, file)
        }
    }
}