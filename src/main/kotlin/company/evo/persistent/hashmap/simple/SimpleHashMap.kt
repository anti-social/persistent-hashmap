package company.evo.persistent.hashmap.simple

import company.evo.persistent.hashmap.BucketLayout_K_V
import company.evo.persistent.hashmap.PAGE_SIZE
import company.evo.persistent.hashmap.PRIMES
import company.evo.persistent.hashmap.Serializer_K
import company.evo.persistent.hashmap.Serializer_V

import java.nio.ByteBuffer

open class PersistentHashMapException(msg: String, cause: Exception? = null) : Exception(msg, cause)
class InvalidHashtableException(msg: String) : PersistentHashMapException(msg)

typealias K = Int
typealias V = Float

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
            return (PAGE_SIZE  - SimpleHashMap_K_V.DATA_PAGE_HEADER_SIZE) / bucketSize
        }
    }
}

abstract class StatsCollector {
    var totalGet = 0L
        protected set
    var foundGet = 0L
        protected set
    var missedGet = 0L
        protected set
    var maxGetDistance = 0
        protected set
    val avgGetDistance
        get() = totalGetDistance.toFloat() / totalGet

    protected var totalGetDistance = 0L

    abstract fun addGet(found: Boolean, dist: Int)

    override fun toString(): String {
        return """
            |total gets: $totalGet
            |found gets: $foundGet
            |missed gets: $missedGet
            |max get distance: $maxGetDistance
            |avg get distance: $avgGetDistance
        """.trimMargin()
    }
}

class DummyStatsCollector : StatsCollector() {
    override fun addGet(found: Boolean, dist: Int) {}
}

class DefaultStatsCollector : StatsCollector() {
    override fun addGet(found: Boolean, dist: Int) {
        totalGet++
        if (found) {
            foundGet++
        } else {
            missedGet++
        }
        totalGetDistance += dist
        if (dist > maxGetDistance) {
            maxGetDistance = dist
        }
    }

}

interface SimpleHashMapRO_K_V {
    val version: Long
    val maxEntries: Int
    val capacity: Int
    val tombstones: Int
    val header: SimpleHashMap_K_V.Header_K_V

    fun get(key: K, defaultValue: V): V
    fun size(): Int

    fun stats(): StatsCollector

    companion object {
        fun fromEnv(env: SimpleHashMapROEnv_K_V, buffer: ByteBuffer): SimpleHashMapRO_K_V {
            return SimpleHashMapROImpl_K_V(
                    env.getCurrentVersion(),
                    buffer,
                    env.bucketLayout,
                    if (env.collectStats) DefaultStatsCollector() else DummyStatsCollector()
            )
        }
    }
}

interface SimpleHashMap_K_V : SimpleHashMapRO_K_V {
    fun put(key: K, value: V): PutResult
    fun remove(key: K): Boolean

    companion object {
        const val DATA_PAGE_HEADER_SIZE = 16

        const val META_SIZE = 2
        const val META_TAG_BITS = 2
        const val META_TAG_SHIFT = 14
        const val META_TAG_MASK = ((1 shl META_TAG_BITS) - 1) shl META_TAG_SHIFT
        const val META_FREE = 0x0000
        const val META_OCCUPIED = 0x8000
        const val META_TOMBSTONE = 0x4000
        const val VER_TAG_BITS = 14
        const val VER_TAG_MASK = (1 shl VER_TAG_BITS) - 1

        fun initBuffer(
                buffer: ByteBuffer,
                bucketLayout: BucketLayout_K_V,
                mapInfo: MapInfo
        ) {
            val header = SimpleHashMap_K_V.Header_K_V(bucketLayout, mapInfo.capacity, mapInfo.maxEntries)
            header.dump(buffer)
        }

        fun fromEnv(env: SimpleHashMapEnv_K_V, buffer: ByteBuffer): SimpleHashMap_K_V {
            return SimpleHashMapImpl_K_V(
                    env.getCurrentVersion(),
                    buffer,
                    env.bucketLayout
            )
        }

        fun bucketLayout_K_V(): BucketLayout_K_V {
            return BucketLayout_K_V(META_SIZE)
        }

//        inline fun <reified V> bucketLayout_K_V(): BucketLayout_K_V {
//            return BucketLayout_K_V(META_SIZE)
//        }
    }

    class Header_K_V(
            val bucketLayout: BucketLayout_K_V,
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

            fun load(buffer: ByteBuffer, bucketLayout: BucketLayout_K_V): Header_K_V {
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
                return Header_K_V(
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

            private fun calcFlags(
                    keySerializer: Serializer_K, valueSerializer: Serializer_V
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

open class SimpleHashMapROImpl_K_V
@JvmOverloads constructor(
        override val version: Long,
        protected val buffer: ByteBuffer,
        protected val bucketLayout: BucketLayout_K_V,
        protected val statsCollector: StatsCollector = DummyStatsCollector()
) : SimpleHashMapRO_K_V {

    protected val keySerializer = bucketLayout.keySerializer
    protected val valueSerializer = bucketLayout.valueSerializer
    val bucketsPerPage = MapInfo.calcBucketsPerPage(bucketLayout.size)

    override val header = SimpleHashMap_K_V.Header_K_V.load(buffer, bucketLayout)

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
    override fun size() = buffer.getInt(SimpleHashMap_K_V.Header_K_V.SIZE_OFFSET)

    override fun stats() = statsCollector

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
        buffer.putInt(SimpleHashMap_K_V.Header_K_V.SIZE_OFFSET, size)
    }

    protected fun readTombstones(): Int {
        return buffer.getInt(SimpleHashMap_K_V.Header_K_V.TOMBSTONES_OFFSET)
    }

    protected fun writeTombstones(tombstones: Int) {
        buffer.putInt(SimpleHashMap_K_V.Header_K_V.TOMBSTONES_OFFSET, tombstones)
    }

    override fun get(key: K, defaultValue: V): V {
        val hash = keySerializer.hash(key)
        find(
                hash,
                maybeFound = { bucketOffset, _, meta, dist ->
//                    when (key) {
//                        bucketLayout.readKey(buffer, bucketOffset) -> {
//                            statsCollector.addGet(true, dist)
//                            return bucketLayout.readValue(buffer, bucketOffset)
//                        }
//                        else -> {
//                            false
//                        }
//                    }
                    var m = meta
                    var value: V
                    while (true) {
                        value = when (key) {
                            bucketLayout.readKey(buffer, bucketOffset) -> {
                                bucketLayout.readValue(buffer, bucketOffset)
                            }
                            else -> {
                                return@find false
                            }
                        }
                        val meta2 = readBucketMeta(bucketOffset)
                        if (m == meta2) {
                            break
                        }
                        m = meta2
                        if (isBucketTombstoned(m) || isBucketEmpty(m)) {
                            return@find false
                        }
                    }
                    statsCollector.addGet(true, dist)
                    return value
                },
                notFound = { _, _, _, dist ->
                    statsCollector.addGet(false, dist)
                    return defaultValue
                }
        )
        return defaultValue
    }

    protected inline fun find(
            hash: Int,
            maybeFound: (offset: Int, bucketIx: Int, meta: Int, dist: Int) -> Boolean,
            notFound: (offset: Int, tombstoneOffset: Int, meta: Int, dist: Int) -> Unit
    ) {
        var dist = -1
        var firstTombstoneBucketOffset = -1
        do {
            dist++
            val bucketIx = getBucketIx(hash, dist)
            val pageOffset = getPageOffset(bucketIx)
            val bucketOffset = getBucketOffset(pageOffset, bucketIx)
            val meta = readBucketMeta(bucketOffset)
            if (isBucketTombstoned(meta)) {
                firstTombstoneBucketOffset = bucketOffset
                continue
            }
            if (isBucketEmpty(meta) /* || dist > SimpleHashMapBaseEnv.MAX_DISTANCE */) {
                notFound(bucketOffset, firstTombstoneBucketOffset, meta, dist)
                return
            }
            if (maybeFound(bucketOffset, bucketIx, meta, dist)) {
                return
            }
        } while (true)
    }

    protected fun getBucketIx(hash: Int, dist: Int): Int {
        val h = (hash + dist) and Int.MAX_VALUE
        return  h % header.capacity
    }

    protected fun nextBucketIx(bucketIx: Int): Int {
        if (bucketIx >= header.capacity - 1) {
            return 0
        }
        return bucketIx + 1
    }

    protected fun prevBucketIx(bucketIx: Int): Int {
        if (bucketIx <= 0) {
            return header.capacity - 1
        }
        return bucketIx - 1
    }

    protected fun readBucketMeta(offset: Int): Int {
        return buffer.getShort(offset + bucketLayout.metaOffset).toInt() and 0xFFFF
    }

    protected fun writeBucketMeta(offset: Int, tag: Int, version: Int) {
        buffer.putShort(
                offset + bucketLayout.metaOffset,
                (tag or (version and SimpleHashMap_K_V.VER_TAG_MASK)).toShort()
        )
    }

    protected fun writeBucketData(offset: Int, key: K, value: V) {
        bucketLayout.writeKey(buffer, offset, key)
        bucketLayout.writeValue(buffer, offset, value)
    }

    protected fun getPageOffset(bucketIx: Int): Int {
        return PAGE_SIZE * (1 + bucketIx / bucketsPerPage)
    }

    protected fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + SimpleHashMap_K_V.DATA_PAGE_HEADER_SIZE +
                (bucketIx % bucketsPerPage) * bucketLayout.size
    }

    protected fun isBucketEmpty(meta: Int) = (meta and SimpleHashMap_K_V.META_TAG_MASK) == 0

    protected fun isBucketOccupied(meta: Int) = (meta and SimpleHashMap_K_V.META_OCCUPIED) != 0

    protected fun isBucketTombstoned(meta: Int) = (meta and SimpleHashMap_K_V.META_TOMBSTONE) != 0

    protected fun bucketVersion(meta: Int) = meta and SimpleHashMap_K_V.VER_TAG_MASK
}

class SimpleHashMapImpl_K_V
@JvmOverloads constructor(
        version: Long,
        buffer: ByteBuffer,
        bucketLayout: BucketLayout_K_V,
        statsCollector: StatsCollector = DummyStatsCollector()
) : SimpleHashMap_K_V, SimpleHashMapROImpl_K_V(
        version,
        buffer,
        bucketLayout,
        statsCollector
) {
    override fun put(key: K, value: V): PutResult {
        val hash = keySerializer.hash(key)
//        println(">>> put($key, $value)")
        find(
                hash,
                maybeFound = { bucketOffset, _, _, _ ->
                    when (key) {
                        bucketLayout.readKey(buffer, bucketOffset) -> {
//                            println("  keys are equal")
                            bucketLayout.writeValue(buffer, bucketOffset, value)
                            true
                        }
                        else -> {
                            false
                        }
                    }
                },
                notFound = { bucketOffset, tombstoneOffset, meta, _ ->
//                    println("> notFound($bucketOffset)")
//                    if (dist > SimpleHashMapBaseEnv.MAX_DISTANCE) {
//                        return PutResult.OVERFLOW
//                    }
                    if (size() >= header.maxEntries) {
                        return PutResult.OVERFLOW
                    }
                    val bucketVersion = bucketVersion(meta)
                    if (tombstoneOffset < 0) {
                        //writeBucketMeta(bucketOffset, SimpleHashMap_K_V.META_FREE, bucketVersion + 1)
                        writeBucketData(bucketOffset, key, value)
                        writeBucketMeta(bucketOffset, SimpleHashMap_K_V.META_OCCUPIED, bucketVersion + 1)
                        writeSize(size() + 1)
                    } else {
                        //writeBucketMeta(bucketOffset, SimpleHashMap_K_V.META_TOMBSTONE, bucketVersion + 1)
                        writeBucketData(tombstoneOffset, key, value)
                        writeBucketMeta(tombstoneOffset, SimpleHashMap_K_V.META_OCCUPIED, bucketVersion + 1)
                        writeTombstones(tombstones - 1)
                        writeSize(size() + 1)
                    }
                }
        )
        return PutResult.OK
    }

    override fun remove(key: K): Boolean {
        val hash = keySerializer.hash(key)
//        println(">>> remove($key)")
        find(
                hash,
                maybeFound = { bucketOffset, bucketIx, meta, _ ->
                    when (key) {
                        bucketLayout.readKey(buffer, bucketOffset) -> {
                            val nextBucketIx = nextBucketIx(bucketIx)
                            val nextBucketPageOffset = getPageOffset(nextBucketIx)
                            val nextBucketOffset = getBucketOffset(nextBucketPageOffset, nextBucketIx)
                            val nextMeta = readBucketMeta(nextBucketOffset)
                            writeBucketMeta(bucketOffset, SimpleHashMap_K_V.META_TOMBSTONE, bucketVersion(meta) + 1)
                            writeTombstones(tombstones + 1)
                            writeSize(size() - 1)
//                            if (isBucketEmpty(nextMeta)) {
//                                writeBucketMeta(bucketOffset, SimpleHashMap_K_V.META_FREE, bucketVersion(meta) + 1)
//                                // writeTombstones(tombstones - cleanupTombstones(bucketIx))
//                                writeSize(size() - 1)
//                            } else {
//                                writeBucketMeta(bucketOffset, SimpleHashMap_K_V.META_TOMBSTONE, bucketVersion(meta) + 1)
//                                // writeTombstones(tombstones + 1)
//                                writeSize(size() - 1)
//                            }
                            return true
                        }
                        else -> {
                            false
                        }
                    }
                },
                notFound = { _, _, _, _ ->
                    return false
                }
        )
        return false
    }

    private fun cleanupTombstones(bucketIx: Int): Int {
        var curBucketIx = bucketIx
        var cleaned = 0
        while (true) {
            val prevBucketIx = prevBucketIx(curBucketIx)
            val pageOffset = getPageOffset(prevBucketIx)
            val prevBucketOffset = getBucketOffset(pageOffset, prevBucketIx)
            val meta = readBucketMeta(prevBucketOffset)
            if (!isBucketTombstoned(meta)) {
                break
            }
            writeBucketMeta(prevBucketOffset, SimpleHashMap_K_V.META_FREE, bucketVersion(meta) + 1)
            cleaned++
            curBucketIx = prevBucketIx
        }

        return cleaned
    }
}
