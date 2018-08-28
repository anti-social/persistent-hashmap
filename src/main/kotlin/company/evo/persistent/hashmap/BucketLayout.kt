package company.evo.persistent.hashmap

import java.nio.ByteBuffer
import kotlin.math.max

class BucketLayout<K, V>(
        val keySerializer: Serializer<K>,
        val valueSerializer: Serializer<V>,
        val metaSize: Int
) {
    companion object {
        inline operator fun <reified K, reified V> invoke(metaSize: Int): BucketLayout<K, V> {
            return BucketLayout(
                    Serializer.getForClass(K::class.java),
                    Serializer.getForClass(V::class.java),
                    metaSize
            )
        }

    }
    val keySize = keySerializer.size
    val valueSize = valueSerializer.size

    // Let's meta offset will be zero
    val metaOffset: Int = 0
    val keyOffset: Int
    val valueOffset: Int
    val size: Int

    init {
        val bucketSize: Int
        if (keySize <= valueSize) {
            keyOffset = metaSize + max(0, keySize - metaSize)
            val baseValueOffset = keyOffset + keySize
            valueOffset = baseValueOffset + max(0, valueSize - baseValueOffset)
            bucketSize = valueOffset + valueSize
        } else {
            valueOffset = metaSize + max(0, valueSize - metaSize)
            val baseKeyOffset = valueOffset + valueSize
            keyOffset = baseKeyOffset + max(0, keySize - baseKeyOffset)
            bucketSize = keyOffset + keySize
        }
        val align = maxOf(metaSize, keySize, valueSize)
        size = ((bucketSize - 1) / align + 1) * align
    }

    fun readKey(buffer: ByteBuffer, bucketOffset: Int): K {
        return keySerializer.read(buffer, bucketOffset + keyOffset)
    }

    fun readRawKey(buffer: ByteBuffer, bucketOffset: Int): ByteArray {
        val array = ByteArray(keySize)
        buffer.position(bucketOffset + keyOffset)
        buffer.get(array)
        return array
    }

    fun readValue(buffer: ByteBuffer, bucketOffset: Int): V {
        return valueSerializer.read(buffer, bucketOffset + valueOffset)
    }

    fun readRawValue(buffer: ByteBuffer, bucketOffset: Int): ByteArray {
        val array = ByteArray(valueSize)
        buffer.position(bucketOffset + valueOffset)
        buffer.get(array)
        return array
    }

    fun writeKey(buffer: ByteBuffer, bucketOffset: Int, key: K) {
        keySerializer.write(buffer, bucketOffset + keyOffset, key)
    }

    fun writeValue(buffer: ByteBuffer, bucketOffset: Int, value: V) {
        valueSerializer.write(buffer, bucketOffset + valueOffset, value)
    }

    override fun toString(): String {
        return "BucketLayout<" +
                "metaOffset = $metaOffset, metaSize = $metaSize, " +
                "keyOffset = $keyOffset, keySize = $keySize, " +
                "valueOffset = $valueOffset, valueSize = $valueSize, " +
                "size = $size>"
    }
}
