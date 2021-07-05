package company.evo.persistent.hashmap.straight

import company.evo.io.MutableUnsafeBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.Dummy32
import company.evo.persistent.hashmap.Hash32
import company.evo.persistent.hashmap.Hash64
import company.evo.persistent.hashmap.Hasher_Int
import company.evo.persistent.hashmap.Knuth32
import company.evo.rc.AtomicRefCounted

import io.kotest.core.spec.style.StringSpec
import io.kotest.data.forAll
import io.kotest.data.headers
import io.kotest.data.row
import io.kotest.data.table
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.RandomSource
import io.kotest.property.Sample
import io.kotest.property.arbitrary.arbitrary

import java.nio.ByteBuffer
import java.util.Random
import kotlin.random.nextInt
import kotlinx.coroutines.yield

//import io.kotlintest.properties.Gen
//import io.kotlintest.shouldBe
//import io.kotlintest.specs.StringSpec
//import io.kotlintest.tables.forAll
//import io.kotlintest.tables.headers
//import io.kotlintest.tables.row
//import io.kotlintest.tables.table

class StraightHashMapTests : StringSpec() {
    // private val seed = System.getProperty("test.random.seed")?.toLong() ?: Random().nextLong()
    // private val random = Random(seed)

    init {
        // println("The seed for <$this> test cases is: $seed")

        "int float: test overflow" {
            createMap_Int_Float(5, Hash32).use { map ->

                map.maxEntries shouldBe 5
                map.capacity shouldBe 7
                map.tombstones() shouldBe 0
                map.size() shouldBe 0

                map.put(1, 1.1F) shouldBe PutResult.OK
                map.get(1, 0.0F) shouldBe 1.1F
                map.tombstones() shouldBe 0
                map.size() shouldBe 1

                map.put(2, 1.2F) shouldBe PutResult.OK
                map.put(3, 1.3F) shouldBe PutResult.OK
                map.put(4, 1.4F) shouldBe PutResult.OK
                map.put(5, 1.5F) shouldBe PutResult.OK
                map.put(6, 1.6F) shouldBe PutResult.OVERFLOW
                map.tombstones() shouldBe 0
                map.size() shouldBe 5

                map.remove(4) shouldBe true
                map.tombstones() shouldBe 1
                map.size() shouldBe 4

                map.remove(5) shouldBe true
                map.tombstones() shouldBe 1
                map.size() shouldBe 3

                map.remove(5) shouldBe false
                map.tombstones() shouldBe 1
                map.size() shouldBe 3

                map.put(6, 1.6F) shouldBe PutResult.OK
                map.tombstones() shouldBe 0
                map.size() shouldBe 4

                map.put(11, 1.11F) shouldBe PutResult.OK
                map.tombstones() shouldBe 0
                map.size() shouldBe 5

                map.put(7, 1.7F) shouldBe PutResult.OVERFLOW
                map.tombstones() shouldBe 0
                map.size() shouldBe 5

                map.put(6, 1.66F) shouldBe PutResult.OK
                map.tombstones() shouldBe 0
                map.size() shouldBe 5

                map.get(1, 0.0F) shouldBe 1.1F
                map.get(2, 0.0F) shouldBe 1.2F
                map.get(3, 0.0F) shouldBe 1.3F
                map.get(11, 0.0F) shouldBe 1.11F
                map.get(6, 0.0F) shouldBe 1.66F
            }
        }

        "int float: skip tombstone when putting existing key" {
            createMap_Int_Float(5, Hash32).use { map ->
                map.capacity shouldBe 7

                map.put(0, 1.0F)
                map.put(7, 1.7F)
                map.remove(0)
                map.put(7, 7.0F)
                map.tombstones() shouldBe 1
                map.size() shouldBe 1
            }
        }

        "int float: no tombstone when removing last record in chain" {
            createMap_Int_Float(5, Hash32).use { map ->
                map.capacity shouldBe 7

                map.put(0, 1.0F)
                map.remove(0)
                map.tombstones() shouldBe 0
                map.size() shouldBe 0
            }
        }

        "int float: cleanup tombstones when removing last record in chain" {
            createMap_Int_Float(5, Hash32).use { map ->
                map.capacity shouldBe 7

                map.put(0, 1.0F)
                map.put(7, 1.7F)
                map.put(9, 1.9F)
                map.tombstones() shouldBe 0
                map.size() shouldBe 3

                map.remove(0)
                map.tombstones() shouldBe 1
                map.size() shouldBe 2

                map.remove(7)
                map.tombstones() shouldBe 2
                map.size() shouldBe 1

                map.remove(9)
                map.tombstones() shouldBe 0
                map.size() shouldBe 0
            }
        }

        "int float: put and remove a little random entries, then get them all".config(invocations = 100) {
            testRandomPutRemove_Int_Float(12, 17)
        }

        "int float: put and remove some random entries, then get them all".config(invocations = 10) {
            testRandomPutRemove_Int_Float(100, 163)
        }

        "int float: put and remove a bunch of random entries, then get them all".config(invocations = 1) {
            testRandomPutRemove_Int_Float(1_000_000, 1395263)
        }

        "long double: put and remove a little random entries, then get them all".config(invocations = 100) {
            testRandomPutRemove_Long_Double(12, 17)
        }

        "long double: put and remove some random entries, then get them all".config(invocations = 10) {
            testRandomPutRemove_Long_Double(100, 163)
        }

        "long double: put and remove a bunch of random entries, then get them all".config(invocations = 1) {
            testRandomPutRemove_Int_Float(1_000_000, 1395263)
        }
    }

    private fun testRandomPutRemove_Int_Float(
            limit: Int, expectedCapacity: Int,
            generateTestCaseCode: Boolean = false
    ) {
        forAll(table(
            headers("hasher"),
            row(Hash32),
            row(Knuth32),
            row(Dummy32)
        )) { hasher ->
            val entries = hashMapOf<Int, Float>()

            if (generateTestCaseCode) {
                println("// Generate data")
                println("val map = createMap_Int_Float<Int, Float>($limit)")
            }
            createMap_Int_Float(limit, hasher).use { map ->
                map.maxEntries shouldBe limit
                map.capacity shouldBe expectedCapacity

                val lastKeys = IntArray((expectedCapacity / 10).coerceAtLeast(1))
                var lastKeyIx = 0
                val keysGen = arbitrary { rs ->
                    val v = rs.random.nextInt(0, limit / 2)
                    when (rs.random.nextInt(5)) {
                        in 0..4 -> {
                            val key = rs.random.nextInt(limit * 10)
                            lastKeys[lastKeyIx % lastKeys.size] = key
                            lastKeyIx++
                            Action.Put(key, rs.random.nextFloat())
                        }
                        else -> {
                            when (rs.random.nextInt(3)) {
                                0 -> {
                                    val key = lastKeys[(lastKeyIx % lastKeys.size).coerceAtMost(lastKeyIx)]
                                    Action.Put(key, rs.random.nextFloat())
                                }
                                1 -> {
                                    val key = lastKeys[(lastKeyIx % lastKeys.size).coerceAtMost(lastKeyIx)]
                                    Action.Remove(key)
                                }
                                else -> {
                                    val key = rs.random.nextInt(limit * 10)
                                    Action.Remove(key)
                                }
                            }

                        }
                    }
                }

                keysGen.samples().take(limit).forEach { s ->
                    when (val a = s.value) {
                        is Action.Put -> {
                            entries.put(a.key, a.value)
                            if (generateTestCaseCode) {
                                println("map.put(${a.key}, ${a.value}F) shouldBe PutResult.OK")
                            }
                            map.put(a.key, a.value) shouldBe PutResult.OK
                        }
                        is Action.Remove -> {
                            val removeRes = entries.remove(a.key) != null
                            if (generateTestCaseCode) {
                                println("map.remove(${a.key}) shouldBe $removeRes")
                            }
                            map.remove(a.key) shouldBe removeRes
                        }
                    }
                }

                if (generateTestCaseCode) {
                    println("// Assertions")
                }

                if (generateTestCaseCode) {
                    println("map.size() shouldBe ${entries.size}")
                }
                map.size() shouldBe entries.size
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
    }

    private fun testRandomPutRemove_Long_Double(
            limit: Int, expectedCapacity: Int,
            generateTestCaseCode: Boolean = false
    ) {
        val entries = hashMapOf<Long, Double>()
        val defaultValue = Double.MIN_VALUE

        if (generateTestCaseCode) {
            println("// Generate data")
            println("val map = createMap_Long_Double($limit)")
        }
        createMap_Long_Double(limit).use { map ->
            map.maxEntries shouldBe limit
            map.capacity shouldBe expectedCapacity

            val lastKeys = LongArray((expectedCapacity / 10).coerceAtLeast(1))
            var lastKeyIx = 0
            val keysGen = arbitrary { rs ->
                val v = rs.random.nextLong(0, limit.toLong() / 2)
                when (rs.random.nextInt(5)) {
                    in 0..4 -> {
                        val key = rs.random.nextLong(limit.toLong() * 10)
                        lastKeys[lastKeyIx % lastKeys.size] = key
                        lastKeyIx++
                        Action.Put(key, rs.random.nextDouble())
                    }
                    else -> {
                        when (rs.random.nextInt(3)) {
                            0 -> {
                                val key = lastKeys[(lastKeyIx % lastKeys.size).coerceAtMost(lastKeyIx)]
                                Action.Put(key, rs.random.nextDouble())
                            }
                            1 -> {
                                val key = lastKeys[(lastKeyIx % lastKeys.size).coerceAtMost(lastKeyIx)]
                                Action.Remove(key)
                            }
                            else -> {
                                val key = rs.random.nextLong(limit.toLong() * 10)
                                Action.Remove(key)
                            }
                        }

                    }
                }
            }
            keysGen.samples().take(limit).forEach { s ->
                when (val a = s.value) {
                    is Action.Put -> {
                        entries.put(a.key, a.value)
                        if (generateTestCaseCode) {
                            println("map.put(${a.key}, ${a.value}F) shouldBe PutResult.OK")
                        }
                        map.put(a.key, a.value) shouldBe PutResult.OK
                    }
                    is Action.Remove -> {
                        val removeRes = entries.remove(a.key) != null
                        if (generateTestCaseCode) {
                            println("map.remove(${a.key}) shouldBe $removeRes")
                        }
                        map.remove(a.key) shouldBe removeRes
                    }
                }
            }

            if (generateTestCaseCode) {
                println("// Assertions")
            }

            if (generateTestCaseCode) {
                println("map.size() shouldBe ${entries.size}")
            }
            map.size() shouldBe entries.size
            (0L..limit).forEach { k ->
                val expectedValue = entries[k]
                val v = map.get(k, defaultValue)
                if (generateTestCaseCode) {
                    println("map.get($k, ${defaultValue}) shouldBe ${expectedValue ?: defaultValue}F")
                }
                v shouldBe (expectedValue ?: defaultValue)
            }
        }
    }

    companion object {
        private fun createMap_Int_Float(
                maxEntries: Int, hasher: Hasher_Int, loadFactor: Double = 0.75
        ): StraightHashMap_Int_Float {
            val mapInfo = MapInfo.calcFor(
                    maxEntries, loadFactor, StraightHashMapType_Int_Float.bucketLayout.size
            )
            val buffer = ByteBuffer.allocate(mapInfo.bufferSize)
            mapInfo.initBuffer(
                    MutableUnsafeBuffer(buffer),
                    StraightHashMapType_Int_Float.keySerializer,
                    StraightHashMapType_Int_Float.valueSerializer,
                    hasher
            )
            val file = AtomicRefCounted(
                    MappedFile("<map>", MutableUnsafeBuffer(buffer))
            ) {}
            return StraightHashMapImpl_Int_Float(0L, file)
        }

        private fun createMap_Long_Double(
                maxEntries: Int, loadFactor: Double = 0.75
        ): StraightHashMap_Long_Double {
            val mapInfo = MapInfo.calcFor(
                    maxEntries, loadFactor, StraightHashMapType_Long_Double.bucketLayout.size
            )
            val buffer = ByteBuffer.allocate(mapInfo.bufferSize)
            mapInfo.initBuffer(
                    MutableUnsafeBuffer(buffer),
                    StraightHashMapType_Long_Double.keySerializer,
                    StraightHashMapType_Long_Double.valueSerializer,
                    StraightHashMapType_Long_Double.hasherProvider.getHasher(Hash64.serial)
            )
            val file = AtomicRefCounted(
                    MappedFile("<map>", MutableUnsafeBuffer(buffer))
            ) {}
            return StraightHashMapType_Long_Double.createWritable(0L, file)
        }
    }

    sealed class Action<K, V> {
        class Put<K, V>(val key: K, val value: V) : Action<K, V>()
        class Remove<K, V>(val key: K) : Action<K, V>()
    }
}
