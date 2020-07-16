package company.evo.persistent.hashmap.straight

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.BucketLayout
import company.evo.persistent.hashmap.HasherProvider_Int
import company.evo.persistent.hashmap.Hasher_Int
import company.evo.persistent.hashmap.PAGE_SIZE
import company.evo.persistent.hashmap.PersistentHashMapIterator_Int_Float
import company.evo.persistent.hashmap.PersistentHashMapRO_Int_Float
import company.evo.persistent.hashmap.PersistentHashMapStats
import company.evo.persistent.hashmap.PersistentHashMapType
import company.evo.persistent.hashmap.PersistentHashMap_Int_Float
import company.evo.persistent.hashmap.PutResult
import company.evo.persistent.hashmap.PersistentHashMapBaseEnv
import company.evo.persistent.hashmap.PersistentHashMapEnv
import company.evo.persistent.hashmap.keyTypes.Int.*
import company.evo.persistent.hashmap.valueTypes.Float.*
import company.evo.processor.KeyValueTemplate
import company.evo.rc.RefCounted

typealias StraightHashMapEnv_Int_Float = PersistentHashMapEnv<
    HasherProvider_Int, Hasher_Int, StraightHashMap_Int_Float, StraightHashMapRO_Int_Float
>

object StraightHashMapType_Int_Float :
    PersistentHashMapType<
        HasherProvider_K, Hasher_K, StraightHashMap_Int_Float, StraightHashMapRO_Int_Float
    >
{
    override val hasherProvider = HasherProvider_K
    override val keySerializer = Serializer_K()
    override val valueSerializer = Serializer_V()
    override val bucketLayout = BucketLayout(MapInfo.META_SIZE, keySerializer.size, valueSerializer.size)

    override fun createWritable(
            version: Long,
            file: RefCounted<MappedFile<MutableIOBuffer>>
    ): StraightHashMap_Int_Float {
        return StraightHashMap_Int_Float(version, file)
    }

    override fun createReadOnly(
            version: Long,
            file: RefCounted<MappedFile<IOBuffer>>
    ): StraightHashMapRO_Int_Float {
        return StraightHashMapRO_Int_Float(version, file)
    }

    override fun copyMap(
            fromMap: StraightHashMap_Int_Float,
            toMap: StraightHashMap_Int_Float
    ): Boolean {
        val iterator = fromMap.iterator()
        while (iterator.next()) {
            if (toMap.put(iterator.key(), iterator.value()) == PutResult.OVERFLOW) {
                return false
            }
        }
        return true
    }
}

open class StraightHashMapRO_Int_Float(
    override val version: Long,
    private val file: RefCounted<MappedFile<IOBuffer>>
) : PersistentHashMapRO_Int_Float {

    private val buffer = file.get().buffer
    override val name = file.get().path

    init {
        assert(buffer.size() % PAGE_SIZE == 0) {
            "Buffer length should be a multiple of $PAGE_SIZE"
        }
    }

    protected val bucketLayout = StraightHashMapType_Int_Float.bucketLayout
    protected val bucketSize = bucketLayout.size
    protected val bucketsPerPage = MapInfo.calcBucketsPerPage(bucketSize)

    val header = Header.load<K, V, Hasher_K>(buffer, K::class.java, V::class.java)
    protected val hasher: Hasher_K = header.hasher

    final override val maxEntries = header.maxEntries
    final override val capacity = header.capacity
    protected val maxBucketIx = capacity - 1
    protected val numDataPages = MapInfo.calcDataPages(capacity, bucketsPerPage)

    override fun close() {
        file.release()
    }

    final override fun size() = readSize()
    final override fun tombstones() = readTombstones()

    final override fun loadBookmark(ix: Int): Long = header.loadBookmark(buffer, ix)
    final override fun loadAllBookmarks() = header.loadAllBookmarks(buffer)

    override fun toString() = dump(false)

    override fun dump(dumpContent: Boolean): String {
        val indexPad = capacity.toString().length
        val description = """Header: $header
            |Bucket layout: $bucketLayout
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

    protected fun isBucketFree(meta: Int) = (meta and MapInfo.META_TAG_MASK) == 0

    protected fun isBucketOccupied(meta: Int) = (meta and MapInfo.META_OCCUPIED) != 0

    protected fun isBucketTombstoned(meta: Int) = (meta and MapInfo.META_TOMBSTONE) != 0

    protected fun bucketVersion(meta: Int) = meta and MapInfo.VER_TAG_MASK

    protected fun getBucketIx(hash: Int, dist: Int): Int {
        val h = (hash + dist) and Int.MAX_VALUE
        return  h % capacity
    }

    protected fun nextBucketIx(bucketIx: Int): Int {
        if (bucketIx >= maxBucketIx) {
            return 0
        }
        return bucketIx + 1
    }

    protected fun prevBucketIx(bucketIx: Int): Int {
        if (bucketIx <= 0) {
            return capacity - 1
        }
        return bucketIx - 1
    }

    protected fun getPageOffset(bucketIx: Int): Int {
        return PAGE_SIZE * (1 + bucketIx / bucketsPerPage)
    }

    protected fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + MapInfo.DATA_PAGE_HEADER_SIZE +
                (bucketIx % bucketsPerPage) * bucketSize
    }

    protected fun readSize(): Int {
        return buffer.readInt(Header.SIZE_OFFSET)
    }

    protected fun readTombstones(): Int {
        return buffer.readInt(Header.TOMBSTONES_OFFSET)
    }

    protected fun readBucketMeta(bucketOffset: Int): Int {
        return buffer.readShortVolatile(
                bucketOffset + bucketLayout.metaOffset
        ).toInt() and 0xFFFF
    }

    protected fun readKey(bucketOffset: Int): K {
        return StraightHashMapType_Int_Float.keySerializer.read(
                buffer, bucketOffset + bucketLayout.keyOffset
        )
    }

    protected fun readValue(bucketOffset: Int): V {
        return StraightHashMapType_Int_Float.valueSerializer.read(
                buffer, bucketOffset + bucketLayout.valueOffset
        )
    }

    private fun readRawKey(bucketOffset: Int): ByteArray {
        val rawKey = ByteArray(StraightHashMapType_Int_Float.keySerializer.size)
        buffer.readBytes(bucketOffset + bucketLayout.keyOffset, rawKey)
        return rawKey
    }

    private fun readRawValue(bucketOffset: Int): ByteArray {
        val rawValue = ByteArray(StraightHashMapType_Int_Float.valueSerializer.size)
        buffer.readBytes(bucketOffset + bucketLayout.valueOffset, rawValue)
        return rawValue
    }

    override fun contains(key: K): Boolean {
        find(
                key,
                found = { _, bucketOffset, meta, _ ->
                    var m = meta
                    while (true) {
                        val meta2 = readBucketMeta(bucketOffset)
                        if (m == meta2) {
                            break
                        }
                        m = meta2
                        if (!isBucketOccupied(m) || key != readKey(bucketOffset)) {
                            return false
                        }
                    }
                    return true
                },
                notFound = { _, _, _, _, _ ->
                    return false
                }
        )
        return false
    }

    override fun get(key: K, defaultValue: V): V {
        find(
               key,
               found = { _, bucketOffset, meta, _ ->
                   var meta1 = meta
                   var value: V
                   while (true) {
                       value = readValue(bucketOffset)
                       val meta2 = readBucketMeta(bucketOffset)
                       if (meta1 == meta2) {
                           break
                       }
                       meta1 = meta2
                       if (!isBucketOccupied(meta1) || key != readKey(bucketOffset)) {
                           return defaultValue
                       }
                   }
                   return value
               },
               notFound = { _, _, _, _, _ ->
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
        val hash = hasher.hash(key)
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
            if (isBucketFree(meta) || dist > PersistentHashMapBaseEnv.MAX_DISTANCE) {
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

@KeyValueTemplate(
    keyTypes = ["Int", "Long"],
    valueTypes = ["Short", "Int", "Long", "Double", "Float"]
)
class StraightHashMap_Int_Float(
    version: Long,
    file: RefCounted<MappedFile<MutableIOBuffer>>
) : PersistentHashMap_Int_Float, StraightHashMapRO_Int_Float(version, file) {

    private val buffer = file.get().buffer

    override fun stats(): PersistentHashMapStats {
        var bucketIx = 0
        var entries = 0
        var tombstones = 0
        var maxDist = 0
        var totalDist = 0L
        pagesLoop@for (pageIx in 0 until numDataPages) {
            val pageOffset = PAGE_SIZE * (1 + pageIx)
            for (ix in 0 until bucketsPerPage) {
                val bucketOffset = pageOffset + MapInfo.DATA_PAGE_HEADER_SIZE + ix * bucketSize
                val meta = readBucketMeta(bucketOffset)
                if (isBucketOccupied(meta)) {
                    entries++
                    val key = readKey(bucketOffset)
                    val zeroDistBucketIx = getBucketIx(hasher.hash(key), 0)
                    val dist = if (bucketIx >= zeroDistBucketIx) {
                        bucketIx - zeroDistBucketIx
                    } else {
                        capacity - zeroDistBucketIx + bucketIx
                    }
                    if (dist > maxDist) {
                        maxDist = dist
                    }
                    totalDist += dist
                } else if (isBucketTombstoned(meta)) {
                    tombstones++
                }

                if (bucketIx == maxBucketIx) {
                    break@pagesLoop
                }
                bucketIx++
            }
        }
        return PersistentHashMapStats(entries, tombstones, maxDist, totalDist.toFloat() / entries)
    }

    protected fun writeSize(size: Int) {
        buffer.writeIntOrdered(Header.SIZE_OFFSET, size)
    }

    protected fun writeTombstones(tombstones: Int) {
        buffer.writeIntOrdered(Header.TOMBSTONES_OFFSET, tombstones)
    }

    protected fun writeBucketMeta(bucketOffset: Int, tag: Int, version: Int) {
        buffer.writeShortVolatile(
                bucketOffset + bucketLayout.metaOffset,
                (tag or (version and MapInfo.VER_TAG_MASK)).toShort()
        )
    }

    protected fun writeKey(bucketOffset: Int, key: K) {
        StraightHashMapType_Int_Float.keySerializer.write(
                buffer, bucketOffset + bucketLayout.keyOffset, key
        )
    }

    protected fun writeValue(bucketOffset: Int, value: V) {
        StraightHashMapType_Int_Float.valueSerializer.write(
                buffer, bucketOffset + bucketLayout.valueOffset, value
        )
    }

    protected fun writeBucketData(bucketOffset: Int, key: K, value: V) {
        writeValue(bucketOffset, value)
        writeKey(bucketOffset, key)
    }

    override fun storeBookmark(ix: Int, value: Long) = header.storeBookmark(buffer, ix, value)
    override fun storeAllBookmarks(values: LongArray) = header.storeAllBookmarks(buffer, values)

    override fun flush() {
        buffer.fsync()
    }

    override fun put(key: K, value: V): PutResult {
        find(
                key,
                found = { _, bucketOffset, _, _ ->
                    writeValue(bucketOffset, value)
                    return PutResult.OK
                },
                notFound = { bucketOffset, meta, tombstoneOffset, tombstoneMeta, dist ->
                    if (dist > PersistentHashMapBaseEnv.MAX_DISTANCE) {
                        return PutResult.OVERFLOW
                    }
                    if (size() >= header.maxEntries) {
                        return PutResult.OVERFLOW
                    }
                    if (tombstoneOffset < 0) {
                        writeBucketData(bucketOffset, key, value)
                        writeBucketMeta(bucketOffset, MapInfo.META_OCCUPIED, bucketVersion(meta) + 1)
                        writeSize(size() + 1)
                    } else {
                        writeBucketData(tombstoneOffset, key, value)
                        writeBucketMeta(tombstoneOffset, MapInfo.META_OCCUPIED, bucketVersion(tombstoneMeta) + 1)
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
                        writeBucketMeta(bucketOffset, MapInfo.META_FREE, bucketVersion(meta) + 1)
                        writeTombstones(tombstones() - cleanupTombstones(bucketIx))
                        writeSize(size() - 1)
                    } else {
                        writeBucketMeta(bucketOffset, MapInfo.META_TOMBSTONE, bucketVersion(meta) + 1)
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
            writeBucketMeta(prevBucketOffset, MapInfo.META_FREE, bucketVersion(meta) + 1)
            cleaned++
            curBucketIx = prevBucketIx
        }

        return cleaned
    }

    override fun iterator(): PersistentHashMapIterator_Int_Float {
        return Iterator()
    }

    inner class Iterator : PersistentHashMapIterator_Int_Float {
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
        override fun key(): K {
            if (curBucketIx >= capacity) {
                throw IndexOutOfBoundsException()
            }
            return readKey(curBucketOffset)
        }
        override fun value(): V {
            if (curBucketIx >= capacity) {
                throw IndexOutOfBoundsException()
            }
            return readValue(curBucketOffset)
        }
    }
}
