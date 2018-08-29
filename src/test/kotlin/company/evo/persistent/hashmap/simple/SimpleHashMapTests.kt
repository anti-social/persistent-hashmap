package company.evo.persistent.hashmap.simple

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

import java.nio.ByteBuffer
import java.util.*

class SimpleHashMapTests : StringSpec() {
    init {
        "test overflow".config(enabled = false) {
            val map = createMap<Int, Float>(5)

            map.maxEntries shouldBe 5
            map.capacity shouldBe 7
            map.tombstones shouldBe 0
            map.size() shouldBe 0

            map.put(1, 1.1F) shouldBe PutResult.OK
            map.get(1, 0.0F) shouldBe 1.1F
            map.tombstones shouldBe 0
            map.size() shouldBe 1

            map.put(2, 1.2F) shouldBe PutResult.OK
            map.put(3, 1.3F) shouldBe PutResult.OK
            map.put(4, 1.4F) shouldBe PutResult.OK
            map.put(5, 1.5F) shouldBe PutResult.OK
            map.put(6, 1.6F) shouldBe PutResult.OVERFLOW
            map.tombstones shouldBe 0
            map.size() shouldBe 5

            map.remove(4) shouldBe true
            map.remove(5) shouldBe true
            map.tombstones shouldBe 2
            map.size() shouldBe 3

            map.put(6, 1.6F) shouldBe PutResult.OK
            map.tombstones shouldBe 2
            map.size() shouldBe 4

            map.put(11, 1.11F) shouldBe PutResult.OK
            map.tombstones shouldBe 1
            map.size() shouldBe 5

            map.put(7, 1.7F) shouldBe PutResult.OVERFLOW
            map.tombstones shouldBe 1
            map.size() shouldBe 5

            map.put(6, 1.66F) shouldBe PutResult.OK
            map.tombstones shouldBe 1
            map.size() shouldBe 5

            map.get(1, 0.0F) shouldBe 1.1F
            map.get(2, 0.0F) shouldBe 1.2F
            map.get(3, 0.0F) shouldBe 1.3F
            map.get(11, 0.0F) shouldBe 1.11F
            map.get(6, 0.0F) shouldBe 1.66F
        }

        "put and remove a little random entries, then get them all" {
            val limit = 100
            val maxKey = limit * 2
            testRandomPutRemove(limit, maxKey, 163)
        }

        "put and remove a bunch of random entries, then get them all" {
            val limit = 1_000_000
            val maxKey = limit * 5
            testRandomPutRemove(limit, maxKey, 1395263)
        }
    }

    private fun testRandomPutRemove(
            limit: Int, maxKey: Int, expectedCapacity: Int,
            generateTestCaseCode: Boolean = false
    ) {
        val entries = hashMapOf<Int, Float>()

        if (generateTestCaseCode) {
            println("val map = createMap<Int, Float>($limit)")
        }
        val map = createMap<Int, Float>(limit)
        map.maxEntries shouldBe limit
        map.capacity shouldBe expectedCapacity

        val keys = RANDOM
                .ints(-maxKey, maxKey)
                .limit(limit.toLong())
        keys.forEach { k ->
            if (RANDOM.nextInt(4) == 3) {
                entries.remove(k)
                if (generateTestCaseCode) {
                    println("map.remove($k) shouldBe true")
                }
                map.remove(k) shouldBe true
            } else {
                val v = RANDOM.nextFloat()
                entries.put(k, v)
                if (generateTestCaseCode) {
                    println("map.put($k, ${v}F) shouldBe PutResult.OK")
                }
                map.put(k, v) shouldBe PutResult.OK
            }
        }

        if (generateTestCaseCode) {
            println()
        }

        map.size() shouldBe entries.size
        (-maxKey..maxKey).forEach { k ->
            val expectedValue = entries[k]
            val v = map.get(k, Float.MIN_VALUE)
            if (generateTestCaseCode) {
                println("map.get($k, ${Float.MIN_VALUE}F) shouldBe ${expectedValue ?: Float.MIN_VALUE}F")
            }
            v shouldBe (expectedValue ?: Float.MIN_VALUE)
        }
    }

    companion object {
        private val RANDOM = Random()

        private inline fun <reified K, reified V> createMap(
                maxEntries: Int, loadFactor: Double = 0.75
        ): SimpleHashMap<K, V> {
            val bucketLayout = SimpleHashMap.bucketLayout<K, V>()
            val mapInfo = MapInfo.calcFor(maxEntries, loadFactor, bucketLayout.size)
            val buffer = ByteBuffer.allocate(mapInfo.bufferSize)
            SimpleHashMap.initBuffer(buffer, bucketLayout, mapInfo)
            return SimpleHashMapImpl(0L, buffer, bucketLayout)
        }
    }
}
