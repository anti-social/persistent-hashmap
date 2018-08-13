package company.evo.persistent.hashmap

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path
import java.util.ArrayList
import java.util.concurrent.locks.ReentrantLock

open class PersistentHashMapException(msg: String, cause: Exception? = null) : Exception(msg, cause)
class WriteLockException(msg: String, cause: Exception? = null) : PersistentHashMapException(msg, cause)
class CorruptedVersionFileException(msg: String) : PersistentHashMapException(msg)
class InvalidHashtableException(msg: String) : PersistentHashMapException(msg)

interface SimpleHashMapRO<K, V> {
    val version: Long
    val header: SimpleHashMap.Header
    fun get(key: K, defaultValue: V): V
    fun size(): Int
}

interface SimpleHashMap<K, V> : SimpleHashMapRO<K, V> {
    class Header(
            val capacity: Int,
            val maxEntries: Int,
            val size: Int = 0,
            val tombstones: Int = 0
    ) {
        companion object {
            val MAGIC = "SPHT\r\n\r\n".toByteArray()
            const val CAPACITY_OFFSET = 8
            const val MAX_ENTRIES = 16
            const val SIZE_OFFSET = 24
            const val TOMBSTONES_OFFSET = 32

            fun load(buffer: ByteBuffer): Header {
                val magic = ByteArray(MAGIC.size)
                buffer.get(magic, 0, MAGIC.size)
                if (!magic.contentEquals(MAGIC)) {
                    throw InvalidHashtableException(
                            "Expected ${MAGIC.contentToString()} magic number " +
                                    "but was: ${magic.contentToString()}"
                    )
                }
                val capacity = toIntOrFail(buffer.getLong(CAPACITY_OFFSET), "capacity")
                val maxEntries = toIntOrFail(buffer.getLong(MAX_ENTRIES), "initialEntries")
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
                            "Currently maximum supported $property is: ${Int.MAX_VALUE}"
                    )
                }
                return v.toInt()
            }
        }

        fun dump(buffer: ByteBuffer) {
            buffer.put(MAGIC, 0, MAGIC.size)
            buffer.putLong(CAPACITY_OFFSET, capacity.toLong())
            buffer.putLong(TOMBSTONES_OFFSET, tombstones.toLong())
            buffer.putLong(SIZE_OFFSET, size.toLong())
            buffer.putLong(TOMBSTONES_OFFSET, tombstones.toLong())
        }
    }

    fun put(key: K, value: V): Boolean
    fun remove(key: K): Boolean
}

abstract class SimpleHashMapBaseEnv(
        protected val mapDir: MapDirectory
) : AutoCloseable
{
    companion object {
        const val MAX_RETRIES = 100
        const val PAGE_SIZE = 4096
        const val DATA_PAGE_HEADER_SIZE = 16
        const val MAX_DISTANCE = 1024
    }

    fun getCurrentVersion() = mapDir.readVersion()
}

class SimpleHashMapROEnv<K, V>(
        mapDir: MapDirectory,
        private val keySerializer: Serializer<K>,
        private val valueSerializer: Serializer<V>,
        private val bucketLayout: BucketLayout
) : SimpleHashMapBaseEnv(mapDir) {

    private val lock = ReentrantLock()

    @Volatile
    private var curVersion: Long = 0

    @Volatile
    private var curBuffer: ByteBuffer = mapDir.openMap(mapDir.readVersion())

    fun getMap(): SimpleHashMapRO<K, V> {
        val ver = mapDir.readVersion()
        if (ver != curVersion) {
            if (lock.tryLock()) {
                try {
                    // TODO Do it in a cycle
                    refresh(ver)
                } finally {
                    lock.unlock()
                }
            }
        }

        return SimpleHashMapROImpl(ver, curBuffer.duplicate(), keySerializer, valueSerializer, bucketLayout, -1)
    }

    private fun refresh(ver: Long) {
        curVersion = ver
        curBuffer = mapDir.openMap(ver)
    }

    override fun close() {}
}

class SimpleHashMapEnv<K, V> private constructor(
        mapDir: MapDirectory,
        private val keySerializer: Serializer<K>,
        private val valueSerializer: Serializer<V>,
        private val bucketLayout: BucketLayout,
        private val numDataPages: Int,
        private val versionFileLock: FileLock
) : SimpleHashMapBaseEnv(mapDir) {
    class Builder<K, V>(keyClass: Class<K>, valueClass: Class<V>) {
        private val keySerializer: Serializer<K> = Serializer.getForClass(keyClass)
        private val valueSerializer: Serializer<V> = Serializer.getForClass(valueClass)
        private val bucketLayout: BucketLayout = BucketLayout(keySerializer.size, valueSerializer.size)

        private var capacity = -1
        private var numDataPages = -1
        private var bucketsPerPage = -1

        companion object {
            private const val DEFAULT_LOAD_FACTOR = 0.75
            private val PRIMES = intArrayOf(
                    // http://referencesource.microsoft.com/#mscorlib/system/collections/hashtable.cs,1663
                    163, 197, 239, 293, 353, 431,
                    521, 631, 761, 919, 1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839,
                    7013, 8419, 10103, 12143, 14591, 17519, 21023, 25229, 30293, 36353, 43627, 52361,
                    62851, 75431, 90523, 108_631, 130_363, 156_437, 187_751, 225_307, 270_371, 324_449,
                    389_357, 467_237, 560_689, 672_827, 807_403, 968_897, 1_162_687, 1_395_263,
                    1_674_319, 2_009_191, 2_411_033, 2_893_249, 3_471_899, 4_166_287, 4_999_559,
                    5_999_471, 7_199_369,
                    // C++ stl (gcc) and
                    // http://www.orcca.on.ca/~yxie/courses/cs2210b-2011/htmls/extra/PlanetMath_%20goodhashtable.pdf
                    8_175_383, 12_582_917, 16_601_593, 25_165_843, 33_712_729, 50_331_653, 68_460_391,
                    100_663_319, 139_022_417, 201_326_611, 282_312_799, 402_653_189, 573_292_817,
                    805_306_457, 1_164_186_217, 1_610_612_741, 2_147_483_647
            )
        }

        var initialEntries: Int = PRIMES[0]
            private set
        fun initialEntries(maxEntries: Int) = apply {
            if (maxEntries <= 0) {
                throw IllegalArgumentException(
                        "Maximum number of entries cannot be negative or zero"
                )
            }
            this.initialEntries = maxEntries
        }

        var loadFactor: Double = DEFAULT_LOAD_FACTOR
            private set
        fun loadFactor(loadFactor: Double) = apply {
            if (loadFactor <= 0 || loadFactor > 1) {
                throw IllegalArgumentException(
                        "Load factor must be great than zero and less or equal 1"
                )
            }
            this.loadFactor = loadFactor
        }

        private fun calcCapacity(maxEntries: Int): Int {
            val minCapacity = (maxEntries / loadFactor).toInt()
            return PRIMES.first { it >= minCapacity }
        }

        private fun calcDataPages(capacity: Int, bucketsPerPage: Int): Int {
            return (capacity + bucketsPerPage - 1) / bucketsPerPage
        }

        private fun calcBucketsPerPage(bucketLayout: BucketLayout): Int {
            return PAGE_SIZE / bucketLayout.size
        }

        private fun prepareCreate() {
            capacity = calcCapacity(initialEntries)
            bucketsPerPage = calcBucketsPerPage(bucketLayout)
            numDataPages = calcDataPages(capacity, bucketsPerPage)
        }

        fun open(path: Path): SimpleHashMapEnv<K, V> {
            val verPath = MapDirectory.getVersionPath(path)
            return if (verPath.toFile().exists()) {
                openWritable(path)
            } else {
                create(path)
            }
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv<K, V> {
            val mapDir = MapDirectory(path, Mode.OPEN_RO)
            return SimpleHashMapROEnv(mapDir, keySerializer, valueSerializer, bucketLayout)
        }

        private fun create(path: Path): SimpleHashMapEnv<K, V> {
            prepareCreate()
            val mapDir = MapDirectory(path, Mode.CREATE)
            val verLock = mapDir.acquireLock()
            mapDir.writeVersion(0L)
            println("buffer size: ${calcBufferSize(numDataPages)}")
            val mapBuffer = mapDir.openMap(0L, bufferSize = calcBufferSize(numDataPages))
            val header = SimpleHashMap.Header(capacity, initialEntries)
            header.dump(mapBuffer)
            return SimpleHashMapEnv(mapDir, keySerializer, valueSerializer, bucketLayout, numDataPages, verLock)
        }

        private fun openWritable(path: Path): SimpleHashMapEnv<K, V> {
            val mapDir = MapDirectory(path, Mode.OPEN_RW)
            val verLock = mapDir.acquireLock()
            return SimpleHashMapEnv(mapDir, keySerializer, valueSerializer, bucketLayout, numDataPages, verLock)
        }

    }

    companion object {
        private fun calcBufferSize(numDataPages: Int): Int {
            return (1 + numDataPages) * PAGE_SIZE
        }

    }

    fun getMap(): SimpleHashMap<K, V> {
        val ver = mapDir.readVersion()
        val mapBuffer = mapDir.openMap(ver)
        return SimpleHashMapImpl(ver, mapBuffer, keySerializer, valueSerializer, bucketLayout, -1)
    }

    fun copyMap(map: SimpleHashMap<K, V>): SimpleHashMap<K, V> {
        val newVersion = map.version + 1
        // TODO Write into temporary file then rename
        val mapBuffer = mapDir.openMap(newVersion, bufferSize = calcBufferSize(numDataPages))
        map.header.dump(mapBuffer)
        // TODO Really copy map data
        mapDir.writeVersion(newVersion)
        return getMap()
    }

    override fun close() {
        versionFileLock.release()
    }
}

enum class Mode {
    CREATE, OPEN_RO, OPEN_RW;

    fun mode() = when (this) {
        CREATE, OPEN_RW -> "rw"
        OPEN_RO -> "r"
    }

    fun mapMode(): FileChannel.MapMode = when (this) {
        CREATE, OPEN_RW -> FileChannel.MapMode.READ_WRITE
        OPEN_RO -> FileChannel.MapMode.READ_ONLY
    }
}

class MapDirectory(
        val path: Path,
        val mode: Mode
) {
    companion object {
        private const val VERSION_FILENAME = "hashmap.ver"
        private const val VERSION_LENGTH = 8L

        private fun getMapFilename(version: Long) = "hashmap_$version.data"

        private fun getMapPath(dir: Path, version: Long): Path = dir.resolve(getMapFilename(version))

        internal fun getVersionPath(dir: Path): Path = dir.resolve(VERSION_FILENAME)

        private fun getVersionBuffer(dir: Path, mode: Mode): ByteBuffer {
            return RandomAccessFile(getVersionPath(dir).toString(), mode.mode())
                    .use { file ->
                        when (mode) {
                            Mode.CREATE -> file.setLength(VERSION_LENGTH)
                            Mode.OPEN_RO, Mode.OPEN_RW -> {
                                if (file.length() != VERSION_LENGTH) {
                                    throw CorruptedVersionFileException(
                                            "Version file must have size $VERSION_LENGTH"
                                    )
                                }
                            }
                        }
                        file.channel.use { channel ->
                            channel
                                    .map(mode.mapMode(), 0, channel.size())
                                    .order(ByteOrder.nativeOrder())
                        }
                    }
        }
    }

    val versionPath = getVersionPath(path)
    private val versionBuffer = getVersionBuffer(path, mode)

    fun readVersion() = versionBuffer.getLong(0)

    fun writeVersion(version: Long) = versionBuffer.putLong(0, version)

    fun acquireLock(): FileLock {
        val lockChannel = RandomAccessFile(versionPath.toString(), "rw").channel
        return try {
            lockChannel.tryLock()
                    ?: throw WriteLockException("Cannot acquire a write lock of the file: $path")
        } catch (e: OverlappingFileLockException) {
            throw WriteLockException("Cannot acquire a write lock of the file: $path", e)
        }
    }

    fun openMap(version: Long, bufferSize: Int = 0): ByteBuffer {
        return RandomAccessFile(getMapPath(path, version).toString(), mode.mode()).use { file ->
            if (bufferSize > 0) {
                file.setLength(bufferSize.toLong())
            }
            file.channel.use { channel ->
                println("Channel size: ${channel.size()}")
                channel
                        .map(mode.mapMode(), 0, channel.size())
                        .order(ByteOrder.nativeOrder())
            }
        }
    }
}

open class SimpleHashMapROImpl<K, V>(
        override val version: Long,
        protected val buffer: ByteBuffer,
        protected val keySerializer: Serializer<K>,
        protected val valueSerializer: Serializer<V>,
        protected val bucketLayout: BucketLayout,
        protected val bucketsPerPage: Int
) : SimpleHashMapRO<K, V> {

    companion object {
        const val META_OCCUPIED = 0x8000
        const val META_THUMBSTONE = 0x4000
    }

    override val header = SimpleHashMap.Header.load(buffer)

    override fun size() = buffer.getInt(SimpleHashMap.Header.SIZE_OFFSET)

    protected fun writeSize(size: Int) {
        buffer.putInt(SimpleHashMap.Header.SIZE_OFFSET, size)
    }

    override fun get(key: K, defaultValue: V): V {
        TODO("not implemented")
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

class SimpleHashMapImpl<K, V>(
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
    override fun put(key: K, value: V): Boolean {
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
                        return false
                    }
                    if (size() >= header.maxEntries) {
                        return false
                    }
                    writeBucketData(bucketOffset, key, value)
                    writeBucketMeta(bucketOffset, META_OCCUPIED)
                    writeSize(size() + 1)
                }
        )
        return true
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
