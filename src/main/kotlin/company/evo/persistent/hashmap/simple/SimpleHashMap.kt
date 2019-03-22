package company.evo.persistent.hashmap.simple

import company.evo.persistent.MappedFile
import company.evo.persistent.RefCounted
import company.evo.persistent.hashmap.BucketLayout
import company.evo.persistent.hashmap.PAGE_SIZE
import company.evo.persistent.hashmap.PRIMES
import company.evo.persistent.hashmap.Serializer_Int
import company.evo.persistent.hashmap.Serializer_Float

import org.agrona.DirectBuffer
import org.agrona.concurrent.AtomicBuffer

open class PersistentHashMapException(msg: String, cause: Exception? = null) : Exception(msg, cause)
class InvalidHashtableException(msg: String) : PersistentHashMapException(msg)

internal typealias K = Int
internal typealias V = Float

internal typealias Serializer_K = Serializer_Int
internal typealias Serializer_V = Serializer_Float

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
            return (PAGE_SIZE  - SimpleHashMap_Int_Float.DATA_PAGE_HEADER_SIZE) / bucketSize
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

interface SimpleHashMapRO_Int_Float : AutoCloseable {
    val version: Long
    val maxEntries: Int
    val capacity: Int
    val header: SimpleHashMap_Int_Float.Header

    fun get(key: K, defaultValue: V): V
    fun size(): Int
    fun tombstones(): Int

    fun stats(): StatsCollector
    fun dump(dumpContent: Boolean): String

    companion object {
        fun create(
                ver: Long, file: RefCounted<MappedFile>, collectStats: Boolean = false
        ): SimpleHashMapRO_Int_Float {
            return SimpleHashMapROImpl_Int_Float(
                    ver,
                    file,
                    if (collectStats) DefaultStatsCollector() else DummyStatsCollector()
            )
        }
    }
}


interface SimpleHashMapIterator_Int_Float {
    fun next(): Boolean
    fun key(): Int
    fun value(): Float
}

interface SimpleHashMap_Int_Float : SimpleHashMapRO_Int_Float {
    fun put(key: K, value: V): PutResult
    fun remove(key: K): Boolean
    fun iterator(): SimpleHashMapIterator_Int_Float

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

        val keySerializer = Serializer_K()
        val valueSerializer = Serializer_V()
        val bucketLayout = BucketLayout(META_SIZE, keySerializer.size, valueSerializer.size)

        fun initBuffer(
                buffer: AtomicBuffer,
                mapInfo: MapInfo
        ) {
            val header = SimpleHashMap_Int_Float.Header(mapInfo.capacity, mapInfo.maxEntries)
            header.dump(buffer)
        }

        fun create(ver: Long, file: RefCounted<MappedFile>): SimpleHashMap_Int_Float {
            return SimpleHashMapImpl_Int_Float(ver, file)
        }
    }

    class Header(
            val capacity: Int,
            val maxEntries: Int
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

            fun load(buffer: DirectBuffer): Header {
                val magic = ByteArray(MAGIC.size)
                buffer.getBytes(0, magic)
                if (!magic.contentEquals(MAGIC)) {
                    throw InvalidHashtableException(
                            "Expected ${MAGIC.contentToString()} magic number " +
                                    "but was: ${magic.contentToString()}"
                    )
                }
                val flags = buffer.getLong(FLAGS_OFFSET)
                (getKeySerial(flags) to keySerializer.serial).let {
                    (serial, expectedSerial) ->
                    assert(serial == expectedSerial) {
                        "Mismatch key type serial: expected $expectedSerial but was $serial"
                    }
                }
                (getValueSerial(flags) to valueSerializer.serial).let {
                    (serial, expectedSerial) ->
                    assert(serial == expectedSerial) {
                        "Mismatch value type serial: expected $expectedSerial but was $serial"
                    }
                }
                val capacity = toIntOrFail(buffer.getLong(CAPACITY_OFFSET), "capacity")
                val maxEntries = toIntOrFail(buffer.getLong(MAX_ENTRIES_OFFSET), "initialEntries")
                return Header(
                        capacity = capacity,
                        maxEntries = maxEntries
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

        fun dump(buffer: AtomicBuffer) {
            buffer.putBytes(0, MAGIC)
            buffer.putLong(FLAGS_OFFSET, calcFlags(keySerializer, valueSerializer))
            buffer.putLong(CAPACITY_OFFSET, capacity.toLong())
            buffer.putLong(MAX_ENTRIES_OFFSET, maxEntries.toLong())
            buffer.putLong(SIZE_OFFSET, 0)
            buffer.putLong(TOMBSTONES_OFFSET, 0)
        }

        override fun toString(): String {
            return "SimpleHashMap.Header<" +
                    "capacity = $capacity, " +
                    "maxEntries = $maxEntries" +
                    ">"
        }
    }
}

open class SimpleHashMapROImpl_Int_Float
@JvmOverloads constructor(
        override val version: Long,
        private val file: RefCounted<MappedFile>,
        private val statsCollector: StatsCollector = DummyStatsCollector()
) : SimpleHashMapRO_Int_Float {

    private val buffer = file.acquire()
            .buffer

    init {
        assert(buffer.capacity() % PAGE_SIZE == 0) {
            "Buffer length should be a multiple of $PAGE_SIZE"
        }
    }

    val bucketsPerPage = MapInfo.calcBucketsPerPage(SimpleHashMap_Int_Float.bucketLayout.size)

    override val header = SimpleHashMap_Int_Float.Header.load(buffer)

    // FIXME
    override val maxEntries = header.maxEntries
    override val capacity = header.capacity

    override fun close() {
        file.release()
    }

    override fun size() = readSize()
    override fun tombstones() = readTombstones()

    override fun stats() = statsCollector

    override fun toString() = dump(false)

    override fun dump(dumpContent: Boolean): String {
        val indexPad = capacity.toString().length
        val description = """Header: $header
            |Bucket layout: ${SimpleHashMap_Int_Float.bucketLayout}
            |Size: ${size()}
            |Tombstones: ${tombstones()}
        """.trimMargin()
        if (dumpContent) {
            val content = (0 until capacity).joinToString("\n") { bucketIx ->
                val pageOffset = getPageOffset(bucketIx)
                val bucketOffset = getBucketOffset(pageOffset, bucketIx)
                "${bucketIx.toString().padEnd(indexPad)}: " +
                        "0x${readBucketMeta(bucketOffset).toString(16)}, " +
                        "${readRawKey(bucketOffset).joinToString(", ", "[", "]")}, " +
                        readRawValue(bucketOffset).joinToString(", ", "[", "]")
            }
            return "$description\n$content"
        }
        return description
    }

    protected fun isBucketFree(meta: Int) = (meta and SimpleHashMap_Int_Float.META_TAG_MASK) == 0

    protected fun isBucketOccupied(meta: Int) = (meta and SimpleHashMap_Int_Float.META_OCCUPIED) != 0

    protected fun isBucketTombstoned(meta: Int) = (meta and SimpleHashMap_Int_Float.META_TOMBSTONE) != 0

    protected fun bucketVersion(meta: Int) = meta and SimpleHashMap_Int_Float.VER_TAG_MASK

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

    protected fun getPageOffset(bucketIx: Int): Int {
        return PAGE_SIZE * (1 + bucketIx / bucketsPerPage)
    }

    protected fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + SimpleHashMap_Int_Float.DATA_PAGE_HEADER_SIZE +
                (bucketIx % bucketsPerPage) * SimpleHashMap_Int_Float.bucketLayout.size
    }

    protected fun readSize(): Int {
        return buffer.getInt(SimpleHashMap_Int_Float.Header.SIZE_OFFSET)
    }

    protected fun writeSize(size: Int) {
        buffer.putIntOrdered(SimpleHashMap_Int_Float.Header.SIZE_OFFSET, size)
    }

    protected fun readTombstones(): Int {
        return buffer.getInt(SimpleHashMap_Int_Float.Header.TOMBSTONES_OFFSET)
    }

    protected fun writeTombstones(tombstones: Int) {
        buffer.putIntOrdered(SimpleHashMap_Int_Float.Header.TOMBSTONES_OFFSET, tombstones)
    }

    protected fun readBucketMeta(bucketOffset: Int): Int {
        return buffer.getShortVolatile(
                bucketOffset + SimpleHashMap_Int_Float.bucketLayout.metaOffset
        ).toInt() and 0xFFFF
    }

    protected fun writeBucketMeta(bucketOffset: Int, tag: Int, version: Int) {
        buffer.putShortVolatile(
                bucketOffset + SimpleHashMap_Int_Float.bucketLayout.metaOffset,
                (tag or (version and SimpleHashMap_Int_Float.VER_TAG_MASK)).toShort()
        )
    }

    protected fun readKey(bucketOffset: Int): K {
        return SimpleHashMap_Int_Float.keySerializer.read(
                buffer, bucketOffset + SimpleHashMap_Int_Float.bucketLayout.keyOffset
        )
    }

    protected fun readValue(bucketOffset: Int): V {
        return SimpleHashMap_Int_Float.valueSerializer.read(
                buffer, bucketOffset + SimpleHashMap_Int_Float.bucketLayout.valueOffset
        )
    }

    private fun readRawKey(bucketOffset: Int): ByteArray {
        val rawKey = ByteArray(SimpleHashMap_Int_Float.keySerializer.size)
        buffer.getBytes(bucketOffset + SimpleHashMap_Int_Float.bucketLayout.keyOffset, rawKey)
        return rawKey
    }

    private fun readRawValue(bucketOffset: Int): ByteArray {
        val rawValue = ByteArray(SimpleHashMap_Int_Float.valueSerializer.size)
        buffer.getBytes(bucketOffset + SimpleHashMap_Int_Float.bucketLayout.valueOffset, rawValue)
        return rawValue
    }

    protected fun writeKey(bucketOffset: Int, key: K) {
        SimpleHashMap_Int_Float.keySerializer.write(
                buffer, bucketOffset + SimpleHashMap_Int_Float.bucketLayout.keyOffset, key
        )
    }

    protected fun writeValue(bucketOffset: Int, value: V) {
        SimpleHashMap_Int_Float.valueSerializer.write(
                buffer, bucketOffset + SimpleHashMap_Int_Float.bucketLayout.valueOffset, value
        )
    }

    protected fun writeBucketData(bucketOffset: Int, key: K, value: V) {
        writeValue(bucketOffset, value)
        writeKey(bucketOffset, key)
    }

    override fun get(key: K, defaultValue: V): V {
        find(
               key,
               found = { _, bucketOffset, meta, dist ->
                   var m = meta
                   var value: V
                   while (true) {
                       value = readValue(bucketOffset)
                       val meta2 = readBucketMeta(bucketOffset)
                       if (m == meta2) {
                           break
                       }
                       m = meta2
                       if (!isBucketOccupied(m)) {
                           statsCollector.addGet(false, dist)
                           return defaultValue
                       }
                   }
                   statsCollector.addGet(true, dist)
                   return value
               },
               notFound = { _, _, _, _, dist ->
                   statsCollector.addGet(false, dist)
                   return defaultValue
               }
        )
        return defaultValue
    }

    protected inline fun find(
            key: K,
            found: (bucketIx: Int, bucketOffset: Int, meta: Int, dist: Int) -> Unit,
            notFound: (bucketOffset: Int, meta: Int, tombstoneOffset: Int, tombstoneMeta: Int, dist: Int) -> Unit
    ) {
        val hash = SimpleHashMap_Int_Float.keySerializer.hash(key)
        var dist = -1
        var tombstoneBucketOffset = -1
        var tombstoneMeta = -1
        while(true) {
            dist++
            val bucketIx = getBucketIx(hash, dist)
            val pageOffset = getPageOffset(bucketIx)
            val bucketOffset = getBucketOffset(pageOffset, bucketIx)
            val meta = readBucketMeta(bucketOffset)
            if (isBucketTombstoned(meta)) {
                tombstoneBucketOffset = bucketOffset
                tombstoneMeta = meta
                continue
            }
            if (isBucketFree(meta) || dist > SimpleHashMapBaseEnv.MAX_DISTANCE) {
                notFound(bucketOffset, meta, tombstoneBucketOffset, tombstoneMeta, dist)
                break
            }
            if (key == readKey(bucketOffset)) {
                found(bucketIx, bucketOffset, meta, dist)
                break
            }
        }
    }
}

class SimpleHashMapImpl_Int_Float
@JvmOverloads constructor(
        version: Long,
        file: RefCounted<MappedFile>,
        statsCollector: StatsCollector = DummyStatsCollector()
) : SimpleHashMap_Int_Float, SimpleHashMapROImpl_Int_Float(
        version,
        file,
        statsCollector
) {
    inner class Iterator : SimpleHashMapIterator_Int_Float {
        private var curBucketIx = -1
        private var curBucketOffset = -1

        override fun next(): Boolean {
            while (true) {
                curBucketIx++
                if (curBucketIx >= capacity) {
                    return false
                }
                val pageOffset = getPageOffset(curBucketIx)
                val bucketOffset = getBucketOffset(pageOffset, curBucketIx)
                if (isBucketOccupied(readBucketMeta(bucketOffset))) {
                    curBucketOffset = bucketOffset
                    return true
                }
            }
        }
        override fun key(): Int {
            if (curBucketIx >= capacity) {
                throw IndexOutOfBoundsException()
            }
            return readKey(curBucketOffset)
        }
        override fun value(): Float {
            if (curBucketIx >= capacity) {
                throw IndexOutOfBoundsException()
            }
            return readValue(curBucketOffset)
        }
    }

    override fun iterator(): SimpleHashMapIterator_Int_Float {
        return Iterator()
    }

    override fun put(key: K, value: V): PutResult {
        find(
                key,
                found = { _, bucketOffset, _, _ ->
                    writeValue(bucketOffset, value)
                    return PutResult.OK
                },
                notFound = { bucketOffset, meta, tombstoneOffset, tombstoneMeta, dist ->
                    if (dist > SimpleHashMapBaseEnv.MAX_DISTANCE) {
                        return PutResult.OVERFLOW
                    }
                    if (size() >= header.maxEntries) {
                        return PutResult.OVERFLOW
                    }
                    if (tombstoneOffset < 0) {
                        writeBucketData(bucketOffset, key, value)
                        writeBucketMeta(bucketOffset, SimpleHashMap_Int_Float.META_OCCUPIED, bucketVersion(meta) + 1)
                        writeSize(size() + 1)
                    } else {
                        writeBucketData(tombstoneOffset, key, value)
                        writeBucketMeta(tombstoneOffset, SimpleHashMap_Int_Float.META_OCCUPIED, bucketVersion(tombstoneMeta) + 1)
                        writeTombstones(tombstones() - 1)
                        writeSize(size() + 1)
                    }
                }
        )
        return PutResult.OK
    }

    override fun remove(key: K): Boolean {
        find(
                key,
                found = { bucketIx, bucketOffset, meta, _ ->
                    val nextBucketIx = nextBucketIx(bucketIx)
                    val nextBucketPageOffset = getPageOffset(nextBucketIx)
                    val nextBucketOffset = getBucketOffset(nextBucketPageOffset, nextBucketIx)
                    val nextMeta = readBucketMeta(nextBucketOffset)

                    if (isBucketFree(nextMeta)) {
                        writeBucketMeta(bucketOffset, SimpleHashMap_Int_Float.META_FREE, bucketVersion(meta) + 1)
                        writeTombstones(tombstones() - cleanupTombstones(bucketIx))
                        writeSize(size() - 1)
                    } else {
                        writeBucketMeta(bucketOffset, SimpleHashMap_Int_Float.META_TOMBSTONE, bucketVersion(meta) + 1)
                        writeTombstones(tombstones() + 1)
                        writeSize(size() - 1)
                    }
                    return true
                },
                notFound = { _, _, _, _, _ ->
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
            writeBucketMeta(prevBucketOffset, SimpleHashMap_Int_Float.META_FREE, bucketVersion(meta) + 1)
            cleaned++
            curBucketIx = prevBucketIx
        }

        return cleaned
    }
}
