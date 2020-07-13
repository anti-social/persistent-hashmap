package company.evo.persistent.hashmap.robinhood

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
import company.evo.persistent.hashmap.keyTypes.Int.*
import company.evo.persistent.hashmap.valueTypes.Float.*
import company.evo.processor.KeyValueTemplate
import company.evo.rc.RefCounted

// typealias RobinHoodHashMapEnv_Int_Float = RobinHoodHashMapEnv<
//     HasherProvider_Int, Hasher_Int, RobinHoodHashMap_Int_Float, RobinHoodHashMapRO_Int_Float
// >

// @KeyValueTemplate(
//     keyTypes = ["Int", "Long"],
//     valueTypes = ["Short", "Int", "Long", "Double", "Float"]
// )
object RobinHoodHashMapType_Int_Float :
    PersistentHashMapType<
        HasherProvider_K, Hasher_K, RobinHoodHashMap_Int_Float, RobinHoodHashMapRO_Int_Float
    >
{
    override val hasherProvider = HasherProvider_K
    override val keySerializer = Serializer_K()
    override val valueSerializer = Serializer_V()
    override val bucketLayout = BucketLayout(MapInfo.META_SIZE, keySerializer.size, valueSerializer.size)

    override fun createWritable(
        version: Long,
        file: RefCounted<MappedFile<MutableIOBuffer>>
    ): RobinHoodHashMap_Int_Float {
        return RobinHoodHashMap_Int_Float(version, file)
    }

    override fun createReadOnly(
        version: Long,
        file: RefCounted<MappedFile<IOBuffer>>
    ): RobinHoodHashMapRO_Int_Float {
        return RobinHoodHashMapRO_Int_Float(version, file)
    }

    override fun copyMap(
        fromMap: RobinHoodHashMap_Int_Float,
        toMap: RobinHoodHashMap_Int_Float
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

open class RobinHoodHashMapRO_Int_Float(
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

    protected val bucketLayout = RobinHoodHashMapType_Int_Float.bucketLayout
    protected val bucketSize = bucketLayout.size
    protected val bucketsPerPage = MapInfo.calcBucketsPerPage(bucketLayout.size)

    val header = Header.load<K, V, Hasher_K>(buffer, K::class.java, V::class.java)
    protected val hasher: Hasher_K = header.hasher

    final override val maxEntries = header.maxEntries
    final override val capacity = header.capacity
    protected val maxBucketIx = capacity - 1
    protected val numDataPages = MapInfo.calcDataPages(capacity, bucketsPerPage)
    val maxDistance = 1024

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
            |Bucket layout: ${RobinHoodHashMapType_Int_Float.bucketLayout}
            |Size: ${size()}
            |Tombstones: ${tombstones()}
        """.trimMargin()
        if (dumpContent) {
            val content = (0 until capacity).joinToString("\n") { bucketIx ->
                val pageOffset = getPageOffset(bucketIx)
                val bucketOffset = getBucketOffset(pageOffset, bucketIx)
                val meta = readBucketMeta(bucketOffset)
                val tag = when (bucketTag(meta)) {
                    MapInfo.META_TOMBSTONE -> 'T'
                    MapInfo.META_OCCUPIED -> 'C'
                    MapInfo.META_FREE -> 'F'
                    else -> throw IllegalStateException("Unknown tag: ${bucketTag(meta)}")
                }
                val dist = bucketDistance(meta)
                val version = bucketVersion(meta)
                "${bucketIx.toString().padEnd(indexPad)}: " +
                    "$tag, $dist, $version, " +
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

    protected fun bucketTag(meta: Int) = meta and MapInfo.META_TAG_MASK

    protected fun bucketDistance(meta: Int) = meta and MapInfo.DIST_TAG_MASK shr MapInfo.DIST_TAG_SHIFT

    protected fun bucketVersion(meta: Int) = meta and MapInfo.VER_TAG_MASK

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
        return pageOffset + MapInfo.DATA_PAGE_HEADER_SIZE +
            (bucketIx % bucketsPerPage) * bucketLayout.size
    }

    protected fun getBucketOffset(bucketIx: Int): Int {
        return getBucketOffset(getPageOffset(bucketIx), bucketIx)
    }

    protected fun readSize(): Int {
        return buffer.readInt(Header.SIZE_OFFSET)
    }

    protected fun readTombstones(): Int {
        return buffer.readInt(Header.TOMBSTONES_OFFSET)
    }

    protected fun readBucketMeta(bucketOffset: Int): Int {
        return buffer.readIntVolatile(
            bucketOffset + bucketLayout.metaOffset
        )
    }

    protected fun readKey(bucketOffset: Int): K {
        return RobinHoodHashMapType_Int_Float.keySerializer.read(
            buffer, bucketOffset + bucketLayout.keyOffset
        )
    }

    protected fun readValue(bucketOffset: Int): V {
        return RobinHoodHashMapType_Int_Float.valueSerializer.read(
            buffer, bucketOffset + bucketLayout.valueOffset
        )
    }

    private fun readRawKey(bucketOffset: Int): ByteArray {
        val rawKey = ByteArray(RobinHoodHashMapType_Int_Float.keySerializer.size)
        buffer.readBytes(bucketOffset + bucketLayout.keyOffset, rawKey)
        return rawKey
    }

    private fun readRawValue(bucketOffset: Int): ByteArray {
        val rawValue = ByteArray(RobinHoodHashMapType_Int_Float.valueSerializer.size)
        buffer.readBytes(bucketOffset + bucketLayout.valueOffset, rawValue)
        return rawValue
    }

    override fun contains(key: K): Boolean {
        find(
            key,
            found = { _, bucketOffset, meta, dist ->
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
            notFound = { _, _, _, _, _, dist ->
                return false
            }
        )
        return false
    }

    override fun get(key: K, defaultValue: V): V {
        find(
            key,
            found = { _, bucketOffset, meta, dist ->
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
            notFound = { _, _, _, _, _, dist ->
                return defaultValue
            }
        )
        return defaultValue
    }

    protected inline fun find(key: K, found: FoundBucketFn, notFound: NotFoundBacketFn) {
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
            val bucketDist = bucketDistance(meta)
            if (isBucketFree(meta) || bucketDist < dist || dist > maxDistance) {
                notFound(bucketIx, bucketOffset, meta, tombstoneBucketOffset, tombstoneMeta, dist)
                break
            }
            if (
                isBucketTombstoned(meta) &&
                bucketDistance(meta) == dist &&
                tombstoneBucketOffset < 0
            ) {
                tombstoneBucketOffset = bucketOffset
                tombstoneMeta = meta
                continue
            }
            if (key == readKey(bucketOffset)) {
                found(bucketIx, bucketOffset, meta, dist)
                break
            }
        }
    }
}

class RobinHoodHashMap_Int_Float(
    version: Long,
    file: RefCounted<MappedFile<MutableIOBuffer>>
) : PersistentHashMap_Int_Float, RobinHoodHashMapRO_Int_Float(version, file) {

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
                    val dist = bucketDistance(meta)
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

    private fun writeSize(size: Int) {
        buffer.writeIntOrdered(Header.SIZE_OFFSET, size)
    }

    private fun writeTombstones(tombstones: Int) {
        buffer.writeIntOrdered(Header.TOMBSTONES_OFFSET, tombstones)
    }

    private fun writeBucketMeta(bucketOffset: Int, tag: Int, dist: Int, version: Int) {
        buffer.writeIntVolatile(
            bucketOffset + bucketLayout.metaOffset,
            (
                tag or
                    (dist shl MapInfo.DIST_TAG_SHIFT) or
                    (version and MapInfo.VER_TAG_MASK)
            )
        )
    }

    private fun writeKey(bucketOffset: Int, key: K) {
        RobinHoodHashMapType_Int_Float.keySerializer.write(
            buffer, bucketOffset + bucketLayout.keyOffset, key
        )
    }

    private fun writeValue(bucketOffset: Int, value: V) {
        RobinHoodHashMapType_Int_Float.valueSerializer.write(
            buffer, bucketOffset + bucketLayout.valueOffset, value
        )
    }

    private fun writeBucketData(bucketOffset: Int, key: K, value: V) {
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
            notFound = { bucketIx, bucketOffset, meta, tombstoneOffset, tombstoneMeta, dist ->
                if (dist > maxDistance) {
                    return PutResult.OVERFLOW
                }
                if (size() >= header.maxEntries) {
                    return PutResult.OVERFLOW
                }
                if (tombstoneOffset < 0) {
                    if (!isBucketFree(meta)) {
                        if (!prepareBucketPlace(bucketIx)) {
                            return PutResult.OVERFLOW
                        }
                    }
                    writeBucketData(bucketOffset, key, value)
                    writeBucketMeta(bucketOffset, MapInfo.META_OCCUPIED, dist, bucketVersion(meta) + 1)
                    writeSize(size() + 1)
                } else {
                    writeBucketData(tombstoneOffset, key, value)
                    writeBucketMeta(tombstoneOffset, MapInfo.META_OCCUPIED, dist, bucketVersion(tombstoneMeta) + 1)
                    writeTombstones(tombstones() - 1)
                    writeSize(size() + 1)
                }
            }
        )
        return PutResult.OK
    }

    private fun prepareBucketPlace(bucketIx: Int): Boolean {
        val freeBucketIx = findFirstFreeBucketIx(bucketIx)
        if (freeBucketIx < 0) {
            return false
        }

        shiftBuckets(bucketIx, freeBucketIx)
        return true
    }

    private fun findFirstFreeBucketIx(bucketIx: Int): Int {
        // Find first free bucket
        var nextIx = nextBucketIx(bucketIx)
        while (true) {
            val nextPageOffset = getPageOffset(nextIx)
            val nextBucketOffset = getBucketOffset(nextPageOffset, nextIx)
            val nextMeta = readBucketMeta(nextBucketOffset)

            if (isBucketFree(nextMeta)) {
                return nextIx
            }
            if (nextIx == bucketIx) {
                return -1
            }
            nextIx = nextBucketIx(nextIx)
        }
    }

    private fun shiftBuckets(startBucketIx: Int, freeBucketIx: Int) {
        var srcBucketIx = prevBucketIx(freeBucketIx)
        var srcBucketOffset = getBucketOffset(srcBucketIx)
        var dstBucketIx = freeBucketIx
        var dstBucketOffset = getBucketOffset(dstBucketIx)
        while (startBucketIx != dstBucketIx) {
            copyBucket(srcBucketOffset, dstBucketOffset)
            dstBucketIx = srcBucketIx
            dstBucketOffset = srcBucketOffset
            srcBucketIx = prevBucketIx(srcBucketIx)
            srcBucketOffset = getBucketOffset(srcBucketIx)
        }
    }

    private fun copyBucket(srcBucketOffset: Int, dstBucketOffset: Int) {
        val key = readKey(srcBucketOffset)
        val value = readValue(srcBucketOffset)
        val meta = readBucketMeta(srcBucketOffset)
        val tag = bucketTag(meta)
        val dist = bucketDistance(meta)
        val version = bucketVersion(meta)
        writeBucketData(dstBucketOffset, key, value)
        writeBucketMeta(dstBucketOffset, tag, dist + 1, version + 1)
    }

    // private inline fun putBucket(
    //     rootCatalogPage: Page, h: Int, dist: Int,
    //     copyOnWrite: Boolean, writeBucket: (Int) -> Unit
    // ): Boolean {
    //     val bucketIx = getBucketIx(h, dist)
    //
    //     // Find first free bucket
    //     var freeBucketIx = bucketIx
    //     var freeBucketOffset: Int
    //     while (true) {
    //         freeBucketOffset = calculateBucketOffset(rootCatalogPage, freeBucketIx)
    //         val meta = readBucketMeta(freeBucketOffset)
    //         if (!isBucketOccupied(meta)) {
    //             break
    //         }
    //         freeBucketIx = nextBucketIx(freeBucketIx)
    //         // There are no free buckets, hash table is full
    //         if (freeBucketIx == bucketIx) {
    //             return false
    //         }
    //     }
    //
    //     // Shift all buckets between current and free bucket
    //     if (bucketIx != freeBucketIx) {
    //         var dstBucketIx = freeBucketIx
    //         while (true) {
    //             val dstBucketOffset = if (copyOnWrite) {
    //                 getNewBucketOffset(dstBucketIx)
    //             } else {
    //                 calculateBucketOffset(rootCatalogPage, dstBucketIx)
    //             }
    //             if (dstBucketOffset < 0) {
    //                 return false
    //             }
    //             val srcBucketIx = prevBucketIx(dstBucketIx)
    //             val srcBucketOffset = calculateBucketOffset(rootCatalogPage, srcBucketIx)
    //             val srcMeta = readBucketMeta(srcBucketOffset)
    //             val srcDistance = getBucketDistance(srcMeta)
    //
    //             copyBucket(srcBucketOffset, dstBucketOffset)
    //             writeBucketDistance(dstBucketOffset, srcDistance + 1)
    //
    //             if (srcBucketIx == bucketIx) {
    //                 break
    //             }
    //
    //             dstBucketIx = srcBucketIx
    //         }
    //     }
    //
    //     // Write data into current bucket
    //     val bucketOffset = if (copyOnWrite) {
    //         getNewBucketOffset(bucketIx)
    //     } else {
    //         calculateBucketOffset(rootCatalogPage, bucketIx)
    //     }
    //     if (bucketOffset < 0) {
    //         return false
    //     }
    //     writeBucket(bucketOffset)
    //
    //     writeSize(size() + 1)
    //     return true
    // }

    // private fun putNoCommit(key: K, value: V, copyOnWrite: Boolean): Boolean {
    //     val h = keySerializer.hash(key)
    //     val rootCatalogPage = pageManager.getRootCatalogPage()
    //     find(rootCatalogPage, h,
    //         maybeFound = { bucketOffset, _ ->
    //             if (key == readBucketKey(bucketOffset)) {
    //                 writeBucketValue(bucketOffset, value)
    //                 true
    //             } else {
    //                 false
    //             }
    //         },
    //         notFound = { _, dist ->
    //             // TODO Consider to save size into local variable
    //             if (size() >= maxEntries) {
    //                 return false
    //             }
    //             putBucket(rootCatalogPage, h, dist, copyOnWrite) { bucketOffset ->
    //                 writeBucketData(bucketOffset, key, value)
    //                 writeBucketDistance(bucketOffset, dist)
    //             }
    //         }
    //     )
    //     return true
    // }

    override fun remove(key: K): Boolean {
        find(
            key,
            found = { bucketIx, bucketOffset, meta, dist ->
                val nextBucketIx = nextBucketIx(bucketIx)
                val nextBucketPageOffset = getPageOffset(nextBucketIx)
                val nextBucketOffset = getBucketOffset(nextBucketPageOffset, nextBucketIx)
                val nextMeta = readBucketMeta(nextBucketOffset)
                val nextDist = bucketDistance(nextMeta)

                // Free buckets also have zero distance
                if (nextDist == 0) {
                    writeBucketMeta(bucketOffset, MapInfo.META_FREE, 0, bucketVersion(meta) + 1)
                    // Clean up previous contiguous tombstones
                    writeTombstones(tombstones() - cleanupTombstones(bucketIx))
                    writeSize(size() - 1)
                } else {
                    writeBucketMeta(bucketOffset, MapInfo.META_TOMBSTONE, dist, bucketVersion(meta) + 1)
                    writeTombstones(tombstones() + 1)
                    writeSize(size() - 1)
                }
                return true
            },
            notFound = { _, _, _, _, _, _ ->
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
            writeBucketMeta(prevBucketOffset, MapInfo.META_FREE, 0, bucketVersion(meta) + 1)
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
