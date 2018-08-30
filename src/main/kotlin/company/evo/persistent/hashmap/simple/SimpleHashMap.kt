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

data class MapInfo(
        val maxEntries: Int,
        val loadFactor: Double,
        val capacity: Int,
        val bucketsPerPage: Int,
        val numDataPages: Int,
        val bufferSize: Int
) {
    companion object {
        fun calcFor(maxEntries: Int, loadFactor: Double, bucketSize: Int): MapInfo {
            val capacity = calcCapacity(maxEntries, loadFactor)
            val bucketsPerPage = calcBucketsPerPage(bucketSize)
            val numDataPages = calcDataPages(capacity, bucketsPerPage)
            return MapInfo(
                    maxEntries = maxEntries,
                    loadFactor = loadFactor,
                    capacity = capacity,
                    bucketsPerPage = bucketsPerPage,
                    numDataPages = numDataPages,
                    bufferSize = (1 + numDataPages) * PAGE_SIZE
            )
        }

        fun calcCapacity(maxEntries: Int, loadFactor: Double): Int {
            val minCapacity = (maxEntries / loadFactor).toInt()
            return PRIMES.first { it >= minCapacity }
        }

        fun calcDataPages(capacity: Int, bucketsPerPage: Int): Int {
            return (capacity + bucketsPerPage - 1) / bucketsPerPage
        }

        fun calcBucketsPerPage(bucketSize: Int): Int {
            return (PAGE_SIZE  - SimpleHashMap.DATA_PAGE_HEADER_SIZE) / bucketSize
        }
    }
}

interface SimpleHashMapRO<K, V> {
    val version: Long
    val maxEntries: Int
    val capacity: Int
    val tombstones: Int
    val header: SimpleHashMap.Header<K, V>

    fun get(key: K, defaultValue: V): V
    fun size(): Int

    companion object {
        fun <K, V> fromEnv(env: SimpleHashMapROEnv<K, V>, buffer: ByteBuffer): SimpleHashMapRO<K, V> {
            return SimpleHashMapROImpl(
                    env.getCurrentVersion(),
                    buffer,
                    env.bucketLayout
            )
        }
    }
}

interface SimpleHashMap<K, V> : SimpleHashMapRO<K, V> {
    fun put(key: K, value: V): PutResult
    fun remove(key: K): Boolean

    companion object {
        const val DATA_PAGE_HEADER_SIZE = 16

        const val META_SIZE = 2
        const val META_TAG_BITS = 2
        const val META_TAG_SHIFT = 14
        const val META_TAG_MASK = ((1 shl META_TAG_BITS) - 1) shl META_TAG_SHIFT
        const val META_OCCUPIED = 0x8000
        const val META_TOMBSTONE = 0x4000

        fun <K, V> initBuffer(
                buffer: ByteBuffer,
                bucketLayout: BucketLayout<K, V>,
                mapInfo: MapInfo
        ) {
            val header = SimpleHashMap.Header(bucketLayout, mapInfo.capacity, mapInfo.maxEntries)
            header.dump(buffer)
        }

        fun <K, V> fromEnv(env: SimpleHashMapEnv<K, V>, buffer: ByteBuffer): SimpleHashMap<K, V> {
            return SimpleHashMapImpl(
                    env.getCurrentVersion(),
                    buffer,
                    env.bucketLayout
            )
        }

        fun <K, V> bucketLayout(keyClass: Class<K>, valueClass: Class<V>): BucketLayout<K, V> {
            return BucketLayout(keyClass, valueClass, META_SIZE)
        }

        inline fun <reified K, reified V> bucketLayout(): BucketLayout<K, V> {
            return BucketLayout(META_SIZE)
        }
    }

    class Header<K, V>(
            val bucketLayout: BucketLayout<K, V>,
            val capacity: Int,
            val maxEntries: Int,
            val size: Int = 0,
            val tombstones: Int = 0
    ) {
        companion object {
            val MAGIC = "SPHT\r\n\r\n".toByteArray()
            const val FLAGS_OFFSET = 8
            const val CAPACITY_OFFSET = 16
            const val MAX_ENTRIES_OFFSET = 24
            const val SIZE_OFFSET = 32
            const val TOMBSTONES_OFFSET = 40

            // Flags
            private const val TYPE_BITS = 3
            private const val TYPE_MASK = (1L shl TYPE_BITS) - 1
            private const val KEY_TYPE_SHIFT = 0
            private const val VALUE_TYPE_SHIFT = 3

            fun <K, V> load(buffer: ByteBuffer, bucketLayout: BucketLayout<K, V>): Header<K, V> {
                buffer.position(0)
                val magic = ByteArray(MAGIC.size)
                buffer.get(magic, 0, MAGIC.size)
                if (!magic.contentEquals(MAGIC)) {
                    throw InvalidHashtableException(
                            "Expected ${MAGIC.contentToString()} magic number " +
                                    "but was: ${magic.contentToString()}"
                    )
                }
                val flags = buffer.getLong(FLAGS_OFFSET)
                (getKeySerial(flags) to bucketLayout.keySerializer.serial).let {
                    (serial, expectedSerial) ->
                    assert(serial == expectedSerial) {
                        "Mismatch key type serial: expected $expectedSerial but was $serial"
                    }
                }
                (getValueSerial(flags) to bucketLayout.valueSerializer.serial).let {
                    (serial, expectedSerial) ->
                    assert(serial == expectedSerial) {
                        "Mismatch value type serial: expected $expectedSerial but was $serial"
                    }
                }
                val capacity = toIntOrFail(buffer.getLong(CAPACITY_OFFSET), "capacity")
                val maxEntries = toIntOrFail(buffer.getLong(MAX_ENTRIES_OFFSET), "initialEntries")
                val size = toIntOrFail(buffer.getLong(SIZE_OFFSET), "size")
                val thumbstones = toIntOrFail(buffer.getLong(TOMBSTONES_OFFSET), "tombstones")
                return Header(
                        bucketLayout,
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

            private fun <K, V> calcFlags(
                    keySerializer: Serializer<K>, valueSerializer: Serializer<V>
            ): Long {
                return (keySerializer.serial and TYPE_MASK shl KEY_TYPE_SHIFT) or
                        (valueSerializer.serial and TYPE_MASK shl VALUE_TYPE_SHIFT)
            }

            private fun getKeySerial(flags: Long): Long {
                return flags ushr KEY_TYPE_SHIFT and TYPE_MASK
            }

            private fun getValueSerial(flags: Long): Long {
                return flags ushr VALUE_TYPE_SHIFT and TYPE_MASK
            }
        }

        fun dump(buffer: ByteBuffer) {
            buffer.position(0)
            buffer.put(MAGIC, 0, MAGIC.size)
            buffer.putLong(FLAGS_OFFSET, calcFlags(bucketLayout.keySerializer, bucketLayout.valueSerializer))
            buffer.putLong(CAPACITY_OFFSET, capacity.toLong())
            buffer.putLong(MAX_ENTRIES_OFFSET, maxEntries.toLong())
            buffer.putLong(SIZE_OFFSET, size.toLong())
            buffer.putLong(TOMBSTONES_OFFSET, tombstones.toLong())
        }

        override fun toString(): String {
            return "SimpleHashMap.Header<" +
                    "capacity = $capacity, " +
                    "maxEntries = $maxEntries, " +
                    "tombstones = $tombstones, " +
                    "size: $size" +
                    ">"
        }
    }
}

open class SimpleHashMapROImpl<K, V>(
        override val version: Long,
        protected val buffer: ByteBuffer,
        protected val bucketLayout: BucketLayout<K, V>
) : SimpleHashMapRO<K, V> {

    protected val keySerializer = bucketLayout.keySerializer
    protected val valueSerializer = bucketLayout.valueSerializer
    val bucketsPerPage = MapInfo.calcBucketsPerPage(bucketLayout.size)

    override val header = SimpleHashMap.Header.load(buffer, bucketLayout)

    init {
        buffer.position(0)
        assert(buffer.capacity() % PAGE_SIZE == 0) {
            "Buffer length should be a multiple of $PAGE_SIZE"
        }
    }

    override val maxEntries = header.maxEntries
    override val capacity = header.capacity
    override val tombstones: Int
        get() = readTombstones()
    override fun size() = buffer.getInt(SimpleHashMap.Header.SIZE_OFFSET)


    override fun toString(): String {
        val indexPad = capacity.toString().length
        val dump = (0 until capacity).joinToString("\n") { bucketIx ->
            val pageOffset = getPageOffset(bucketIx)
            val bucketOffset = getBucketOffset(pageOffset, bucketIx)
            "${bucketIx.toString().padEnd(indexPad)}: " +
                    "${readBucketMeta(bucketOffset)}, " +
                    "${bucketLayout.readRawKey(buffer, bucketOffset).joinToString(", ", "[", "]")}, " +
                    "${bucketLayout.readRawValue(buffer, bucketOffset).joinToString(", ", "[", "]")}"
        }
        return """Header: $header"
            |Data:
            |$dump
        """.trimMargin()
    }

    protected fun writeSize(size: Int) {
        buffer.putInt(SimpleHashMap.Header.SIZE_OFFSET, size)
    }

    protected fun readTombstones(): Int {
        return buffer.getInt(SimpleHashMap.Header.TOMBSTONES_OFFSET)
    }

    protected fun writeTombstones(tombstones: Int) {
        buffer.putInt(SimpleHashMap.Header.TOMBSTONES_OFFSET, tombstones)
    }

    override fun get(key: K, defaultValue: V): V {
        val hash = keySerializer.hash(key)
//        println(">>> get($key, $defaultValue)")
        find(
                hash,
                maybeFound = { bucketOffset, meta, _ ->
//                    println("  > maybeFound($bucketOffset, $meta)")
                    if (isBucketTombstoned(meta)) {
//                        println("    tombstone")
                        false
                    } else {
                        if (bucketLayout.readKey(buffer, bucketOffset) == key) {
                            return bucketLayout.readValue(buffer, bucketOffset)
                        } else {
                            false
                        }
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
            maybeFound: (offset: Int, meta: Int, distance: Int) -> Boolean,
            notFound: (offset: Int, distance: Int) -> Unit
    ) {
        var dist = -1
        do {
            dist++
            val bucketIx = getBucketIx(hash, dist)
            val pageOffset = getPageOffset(bucketIx)
            val bucketOffset = getBucketOffset(pageOffset, bucketIx)
            val meta = readBucketMeta(bucketOffset)
//            println("    meta: $meta")
//            if (isBucketTombstoned(meta)) {
//                continue
//            }
            if (isBucketEmpty(meta) || dist > SimpleHashMapBaseEnv.MAX_DISTANCE) {
                notFound(bucketOffset, dist)
                return
            }
            if (maybeFound(bucketOffset, meta, dist)) {
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
        return buffer.getShort(offset + bucketLayout.metaOffset).toInt() and 0xFFFF
    }

    protected fun writeBucketMeta(offset: Int, meta: Int) {
        buffer.putShort(offset + bucketLayout.metaOffset, meta.toShort())
    }

    protected fun writeBucketData(offset: Int, key: K, value: V) {
        bucketLayout.writeKey(buffer, offset, key)
        bucketLayout.writeValue(buffer, offset, value)
    }

    protected fun getPageOffset(bucketIx: Int): Int {
        return PAGE_SIZE * (1 + bucketIx / bucketsPerPage)
    }

    protected fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + SimpleHashMap.DATA_PAGE_HEADER_SIZE +
                (bucketIx % bucketsPerPage) * bucketLayout.size
    }

    protected fun isBucketEmpty(meta: Int) = (meta and SimpleHashMap.META_TAG_MASK) == 0

    protected fun isBucketOccupied(meta: Int) = (meta and SimpleHashMap.META_OCCUPIED) != 0

    protected fun isBucketTombstoned(meta: Int) = (meta and SimpleHashMap.META_TOMBSTONE) != 0
}

class SimpleHashMapImpl<K, V> (
        version: Long,
        buffer: ByteBuffer,
        bucketLayout: BucketLayout<K, V>
) : SimpleHashMap<K, V>, SimpleHashMapROImpl<K, V>(
        version,
        buffer,
        bucketLayout
) {
    override fun put(key: K, value: V): PutResult {
        val hash = keySerializer.hash(key)
//        println(">>> put($key, $value)")
        find(
                hash,
                maybeFound = { bucketOffset, meta, _ ->
//                    println("> maybeFound($bucketOffset, $meta)")
                    if (isBucketTombstoned(meta)) {
//                        println("  isBucketTombstoned")
                        writeBucketData(bucketOffset, key, value)
                        writeBucketMeta(bucketOffset, SimpleHashMap.META_OCCUPIED)
                        writeTombstones(tombstones - 1)
                        writeSize(size() + 1)
                    }
                    if (bucketLayout.readKey(buffer, bucketOffset) == key) {
//                        println("  keys are equal")
                        bucketLayout.writeValue(buffer, bucketOffset, value)
                        true
                    } else {
                        false
                    }
                },
                notFound = { bucketOffset, dist ->
//                    println("> notFound($bucketOffset)")
                    if (dist > SimpleHashMapBaseEnv.MAX_DISTANCE) {
                        return PutResult.OVERFLOW
                    }
                    if (size() >= header.maxEntries) {
                        return PutResult.OVERFLOW
                    }
                    writeBucketData(bucketOffset, key, value)
                    writeBucketMeta(bucketOffset, SimpleHashMap.META_OCCUPIED)
                    writeSize(size() + 1)
                }
        )
        return PutResult.OK
    }

    override fun remove(key: K): Boolean {
        val hash = keySerializer.hash(key)
//        println(">>> remove($key)")
        find(
                hash,
                maybeFound = { bucketOffset, meta, _ ->
                    if (isBucketTombstoned(meta)) {
                        return@find false
                    }
                    if (bucketLayout.readKey(buffer, bucketOffset) == key) {
                        writeBucketMeta(bucketOffset, SimpleHashMap.META_TOMBSTONE)
                        writeSize(size() - 1)
                        writeTombstones(tombstones + 1)
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