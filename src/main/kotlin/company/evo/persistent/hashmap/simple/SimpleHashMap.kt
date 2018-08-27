package company.evo.persistent.hashmap.simple

import company.evo.persistent.hashmap.BucketLayout
import company.evo.persistent.hashmap.PAGE_SIZE
import company.evo.persistent.hashmap.PRIMES
import company.evo.persistent.hashmap.Serializer

import java.nio.ByteBuffer

open class PersistentHashMapException(msg: String, cause: Exception? = null) : Exception(msg, cause)
class InvalidHashtableException(msg: String) : PersistentHashMapException(msg)

enum class PutResult {
    OK, OVERFLOW
}

interface SimpleHashMapRO<K, V> {
    val version: Long
    val header: SimpleHashMap.Header
    fun get(key: K, defaultValue: V): V
    fun size(): Int

    companion object {
        fun <K, V> fromEnv(env: SimpleHashMapROEnv<K, V>, buffer: ByteBuffer): SimpleHashMapRO<K, V> {
            return SimpleHashMapROImpl(
                    env.getCurrentVersion(),
                    buffer,
                    env.keySerializer,
                    env.valueSerializer,
                    env.bucketLayout,
                    env.bucketsPerPage
            )
        }
    }
}

interface SimpleHashMap<K, V> : SimpleHashMapRO<K, V> {
    fun put(key: K, value: V): PutResult
    fun remove(key: K): Boolean

    companion object {
        const val DATA_PAGE_HEADER_SIZE = 16

        fun initialize(buffer: ByteBuffer, capacity: Int, initialEntries: Int) {
            val header = SimpleHashMap.Header(capacity, initialEntries)
            header.dump(buffer)
        }

        fun <K, V> fromEnv(env: SimpleHashMapEnv<K, V>, buffer: ByteBuffer): SimpleHashMap<K, V> {
            return SimpleHashMapImpl(
                    env.getCurrentVersion(),
                    buffer,
                    env.keySerializer,
                    env.valueSerializer,
                    env.bucketLayout,
                    env.bucketsPerPage
            )
        }

        fun calcCapacity(maxEntries: Int, loadFactor: Float): Int {
            val minCapacity = (maxEntries / loadFactor).toInt()
            return PRIMES.first { it >= minCapacity }
        }

        fun calcDataPages(capacity: Int, bucketsPerPage: Int): Int {
            return (capacity + bucketsPerPage - 1) / bucketsPerPage
        }

        fun calcBufferSize(numDataPages: Int): Int {
            return (1 + numDataPages) * PAGE_SIZE
        }

        fun calcBucketsPerPage(bucketLayout: BucketLayout): Int {
            return (PAGE_SIZE  - DATA_PAGE_HEADER_SIZE) / bucketLayout.size
        }
    }

    class Header(
            val capacity: Int,
            val maxEntries: Int,
            val size: Int = 0,
            val tombstones: Int = 0
    ) {
        companion object {
            val MAGIC = "SPHT\r\n\r\n".toByteArray()
            const val CAPACITY_OFFSET = 8
            const val MAX_ENTRIES_OFFSET = 16
            const val SIZE_OFFSET = 24
            const val TOMBSTONES_OFFSET = 32

            fun load(buffer: ByteBuffer): Header {
                buffer.position(0)
                val magic = ByteArray(MAGIC.size)
                buffer.get(magic, 0, MAGIC.size)
                if (!magic.contentEquals(MAGIC)) {
                    throw InvalidHashtableException(
                            "Expected ${MAGIC.contentToString()} magic number " +
                                    "but was: ${magic.contentToString()}"
                    )
                }
                val capacity = toIntOrFail(buffer.getLong(CAPACITY_OFFSET), "capacity")
                val maxEntries = toIntOrFail(buffer.getLong(MAX_ENTRIES_OFFSET), "initialEntries")
                val size = toIntOrFail(buffer.getLong(SIZE_OFFSET), "size")
                val thumbstones = toIntOrFail(buffer.getLong(TOMBSTONES_OFFSET), "tombstones")
                return Header(
                        capacity = capacity,
                        maxEntries = maxEntries,
                        size = size,
                        tombstones = thumbstones
                )
            }

            private fun toIntOrFail(v: Long, property: String): Int {
                if (v > Int.MAX_VALUE) {
                    throw InvalidHashtableException(
                            "Currently maximum supported $property is: ${Int.MAX_VALUE}, but was: $v"
                    )
                }
                return v.toInt()
            }
        }

        fun dump(buffer: ByteBuffer) {
            buffer.position(0)
            buffer.put(MAGIC, 0, MAGIC.size)
            buffer.putLong(CAPACITY_OFFSET, capacity.toLong())
            buffer.putLong(MAX_ENTRIES_OFFSET, maxEntries.toLong())
            buffer.putLong(SIZE_OFFSET, size.toLong())
            buffer.putLong(TOMBSTONES_OFFSET, tombstones.toLong())
        }

        override fun toString(): String {
            return """
                |Capacity: $capacity
                |Max entries: $maxEntries
                |Tombstones: $tombstones
                |Size: $size
            """.trimMargin()
        }
    }
}

open class SimpleHashMapROImpl<K, V>(
        override val version: Long,
        protected val buffer: ByteBuffer,
        protected val bucketLayout: BucketLayout
) : SimpleHashMapRO<K, V> {

    companion object {
        const val META_OCCUPIED = 0x8000
        const val META_THUMBSTONE = 0x4000
    }

    val bucketsPerPage = SimpleHashMap.calcBucketsPerPage(bucketLayout)

    override val header = SimpleHashMap.Header.load(buffer)

    override fun size() = buffer.getInt(SimpleHashMap.Header.SIZE_OFFSET)


    override fun toString(): String {
        return """
            |Buffer: $buffer
            |Header: $header
        """.trimMargin()
    }

    protected fun writeSize(size: Int) {
        buffer.putInt(SimpleHashMap.Header.SIZE_OFFSET, size)
    }

    override fun get(key: K, defaultValue: V): V {
        val hash = keySerializer.hash(key)
        find(
                hash,
                maybeFound = { bucketOffset, _ ->
                    if (readBucketKey(bucketOffset) == key) {
                        return readBucketValue(bucketOffset)
                    } else {
                        false
                    }
                },
                notFound = { _, _ ->
                    return defaultValue
                }
        )
        return defaultValue
    }

    protected inline fun find(
            hash: Int,
            maybeFound: (offset: Int, distance: Int) -> Boolean,
            notFound: (offset: Int, distance: Int) -> Unit
    ) {
        var dist = -1
        do {
            dist++
            val bucketIx = getBucketIx(hash, dist)
            val pageOffset = getPageOffset(bucketIx)
            val bucketOffset = getBucketOffset(pageOffset, bucketIx)
            val meta = readBucketMeta(bucketOffset)
            if (isBucketThumbstoned(meta)) {
                continue
            }
            if (!isBucketOccupied(meta) || dist > SimpleHashMapBaseEnv.MAX_DISTANCE) {
                notFound(bucketOffset, dist)
                return
            }
            if (maybeFound(bucketOffset, dist)) {
                return
            }
        } while (true)
    }

    protected fun getBucketIx(hash: Int, dist: Int): Int {
        return (hash + dist) % header.capacity
    }

    protected fun nextBucketIx(bucketIx: Int): Int {
        val nextIx = bucketIx + 1
        if (nextIx >= header.capacity) {
            return 0
        }
        return nextIx
    }

    protected fun readBucketMeta(offset: Int): Int {
        println(">>> readBucketMeta($offset)")
        return buffer.getShort(offset + bucketLayout.metaOffset).toInt() and 0xFFFF
    }

    protected fun readBucketKey(offset: Int): K {
        return keySerializer.read(buffer, offset + bucketLayout.keyOffset)
    }

    protected fun readBucketValue(offset: Int): V {
        return valueSerializer.read(buffer, offset + bucketLayout.valueOffset)
    }

    protected fun writeBucketMeta(offset: Int, meta: Int) {
        buffer.putShort(offset + bucketLayout.metaOffset, meta.toShort())
    }

    protected fun writeBucketKey(offset: Int, key: K) {
        keySerializer.write(buffer, offset + bucketLayout.keyOffset, key)
    }

    protected fun writeBucketValue(offset: Int, value: V) {
        valueSerializer.write(buffer, offset + bucketLayout.valueOffset, value)
    }

    protected fun writeBucketData(offset: Int, key: K, value: V) {
        writeBucketKey(offset, key)
        writeBucketValue(offset, value)
    }

    protected fun getPageOffset(bucketIx: Int): Int {
        return SimpleHashMapBaseEnv.PAGE_SIZE * bucketIx / bucketsPerPage
    }

    protected fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + (bucketIx % bucketsPerPage) * bucketLayout.size
    }

    protected fun isBucketOccupied(meta: Int) = (meta and META_OCCUPIED) != 0

    protected fun isBucketThumbstoned(meta: Int) = (meta and META_THUMBSTONE) != 0
}

class SimpleHashMapImpl<K, V> (
        version: Long,
        buffer: ByteBuffer,
        keySerializer: Serializer<K>,
        valueSerializer: Serializer<V>,
        bucketLayout: BucketLayout,
        bucketsPerPage: Int
) : SimpleHashMap<K, V>, SimpleHashMapROImpl<K, V>(
        version,
        buffer,
        keySerializer,
        valueSerializer,
        bucketLayout,
        bucketsPerPage
) {
    override fun put(key: K, value: V): PutResult {
        val hash = keySerializer.hash(key)
        find(
                hash,
                maybeFound = { bucketOffset, _ ->
                    if (readBucketKey(bucketOffset) == key) {
                        writeBucketValue(bucketOffset, value)
                        true
                    } else {
                        false
                    }
                },
                notFound = { bucketOffset, dist ->
                    if (dist > SimpleHashMapBaseEnv.MAX_DISTANCE) {
                        return PutResult.OVERFLOW
                    }
                    if (size() >= header.maxEntries) {
                        return PutResult.OVERFLOW
                    }
                    writeBucketData(bucketOffset, key, value)
                    writeBucketMeta(bucketOffset, META_OCCUPIED)
                    writeSize(size() + 1)
                }
        )
        return PutResult.OK
    }

    override fun remove(key: K): Boolean {
        val hash = keySerializer.hash(key)
        find(
                hash,
                maybeFound = { bucketOffset, _ ->
                    if (readBucketKey(bucketOffset) == key) {
                        writeBucketMeta(bucketOffset, META_THUMBSTONE)
                        writeSize(size() - 1)
                        true
                    } else {
                        false
                    }
                },
                notFound = { _, _ -> }
        )
        return true
    }
}
