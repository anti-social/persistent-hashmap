package company.evo.persistent.hashmap

import kotlin.math.max

import org.agrona.concurrent.AtomicBuffer

typealias K = Int
typealias V = Float

class BucketLayout_K_V(
        val keySerializer: Serializer_K,
        val valueSerializer: Serializer_V,
        val metaSize: Int
) {
    companion object {
        operator fun invoke(metaSize: Int): BucketLayout_K_V {
            return BucketLayout_K_V(
                    Serializer_K(),
                    Serializer_V(),
                    metaSize
            )
        }

//        inline operator fun <reified V> invoke(metaSize: Int): BucketLayout_Key<V> {
//            return BucketLayout_Key(
//                    Serializer_Key(),
//                    Serializer.getForClass(V::class.java),
//                    metaSize
//            )
//        }
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

    fun readKey(buffer: AtomicBuffer, bucketOffset: Int): K {
        return keySerializer.read(buffer, bucketOffset + keyOffset)
    }

    fun readRawKey(buffer: AtomicBuffer, bucketOffset: Int): ByteArray {
        val array = ByteArray(keySize)
        buffer.getBytes(0, array)
        return array
    }

    fun readValue(buffer: AtomicBuffer, bucketOffset: Int): V {
        return valueSerializer.read(buffer, bucketOffset + valueOffset)
    }

    fun readRawValue(buffer: AtomicBuffer, bucketOffset: Int): ByteArray {
        val array = ByteArray(valueSize)
        buffer.getBytes(0, array)
        return array
    }

    fun writeKey(buffer: AtomicBuffer, bucketOffset: Int, key: K) {
        keySerializer.write(buffer, bucketOffset + keyOffset, key)
    }

    fun writeValue(buffer: AtomicBuffer, bucketOffset: Int, value: V) {
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
