package company.evo.persistent.hashmap.simple

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

import java.nio.ByteBuffer

class SimpleHashMapTests : StringSpec() {
    init {
        "test overflow" {
            val bucketLayout = SimpleHashMap.bucketLayout<Int, Float>()
            val mapInfo = MapInfo.calcFor(5, 0.75, bucketLayout.size)
            val buffer = ByteBuffer.allocate(mapInfo.bufferSize)
            SimpleHashMap.initBuffer(buffer, bucketLayout, mapInfo)
            val map = SimpleHashMapImpl(0L, buffer, bucketLayout)

            map.put(1, 1.1F) shouldBe PutResult.OK
            map.get(1, 0.0F) shouldBe 1.1F

            map.put(2, 1.2F) shouldBe PutResult.OK
            map.put(3, 1.3F) shouldBe PutResult.OK
            map.put(4, 1.4F) shouldBe PutResult.OK
            map.put(5, 1.5F) shouldBe PutResult.OK
            map.put(6, 1.6F) shouldBe PutResult.OVERFLOW

            map.remove(5) shouldBe true
            map.put(6, 1.6F) shouldBe PutResult.OK
            map.put(7, 1.7F) shouldBe PutResult.OVERFLOW
        }
    }
}
