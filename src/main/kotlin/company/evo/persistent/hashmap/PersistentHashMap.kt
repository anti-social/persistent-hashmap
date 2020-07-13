package company.evo.persistent.hashmap

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer
import company.evo.persistent.MappedFile
import company.evo.rc.RefCounted

enum class PutResult {
    OK, OVERFLOW
}

interface PersistentHashMapRO : AutoCloseable {
    val version: Long
    val name: String
    val maxEntries: Int
    val capacity: Int
    fun size(): Int

    fun loadBookmark(ix: Int): Long
    fun loadAllBookmarks(): LongArray
}

interface PersistentHashMap : PersistentHashMapRO {
    fun stats(): PersistentHashMapStats

    fun storeBookmark(ix: Int, value: Long)
    fun storeAllBookmarks(values: LongArray)
}

interface PersistentHashMapType<P: HasherProvider<H>, H: Hasher, W: PersistentHashMap, RO: PersistentHashMapRO> {
    val bucketLayout: BucketLayout
    val keySerializer: Serializer
    val valueSerializer: Serializer
    val hasherProvider: HasherProvider<H>
    fun createWritable(
        version: Long,
        file: RefCounted<MappedFile<MutableIOBuffer>>
    ): W
    fun createReadOnly(
        version: Long,
        file: RefCounted<MappedFile<IOBuffer>>
    ): RO
    fun copyMap(fromMap: W, toMap: W): Boolean
}

data class PersistentHashMapStats(
    val size: Int, val tombstones: Int, val maxDist: Int, val avgDist: Float
)
