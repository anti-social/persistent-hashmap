package company.evo.persistent.hashmap

import kotlin.math.max

class BucketLayout(val keySize: Int, val valueSize: Int) {
    companion object {
        const val META_SIZE = 2
    }

    val metaOffset: Int = 0
    val keyOffset: Int
    val valueOffset: Int
    val size: Int

    init {
        val bucketSize: Int
        if (keySize <= valueSize) {
            keyOffset = META_SIZE + max(0, keySize - META_SIZE)
            val baseValueOffset = keyOffset + keySize
            valueOffset = baseValueOffset + max(0, valueSize - baseValueOffset)
            bucketSize = valueOffset + valueSize
        } else {
            valueOffset = META_SIZE + max(0, valueSize - META_SIZE)
            val baseKeyOffset = valueOffset + valueSize
            keyOffset = baseKeyOffset + max(0, keySize - baseKeyOffset)
            bucketSize = keyOffset + keySize
        }
        size = ((bucketSize - 1) / 8 + 1) * 8
    }
}
