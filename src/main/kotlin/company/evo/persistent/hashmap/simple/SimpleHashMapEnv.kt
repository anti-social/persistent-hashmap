package company.evo.persistent.hashmap.simple

import company.evo.persistent.VersionedDirectory
import company.evo.persistent.hashmap.BucketLayout
import company.evo.persistent.hashmap.Serializer
import java.nio.ByteBuffer
import java.nio.file.Path

import java.util.concurrent.locks.ReentrantLock

abstract class SimpleHashMapBaseEnv(
        protected val dir: VersionedDirectory
) : AutoCloseable
{
    companion object {
        const val MAX_RETRIES = 100
        const val PAGE_SIZE = 4096
        const val DATA_PAGE_HEADER_SIZE = 16
        const val MAX_DISTANCE = 1024

        fun calcBufferSize(numDataPages: Int): Int {
            return (1 + numDataPages) * PAGE_SIZE
        }

        fun getHashmapFilename(version: Long) = "hashmap_$version.data"
    }

    fun getCurrentVersion() = dir.readVersion()
}

class SimpleHashMapROEnv<K, V>(
        dir: VersionedDirectory,
        private val keySerializer: Serializer<K>,
        private val valueSerializer: Serializer<V>,
        private val bucketLayout: BucketLayout
) : SimpleHashMapBaseEnv(dir) {

    private val lock = ReentrantLock()

    @Volatile
    private var curVersion: Long = 0

    @Volatile
    private var curBuffer: ByteBuffer = dir.openFileReadOnly(getHashmapFilename(dir.readVersion()))

    fun getMap(): SimpleHashMapRO<K, V> {
        val ver = dir.readVersion()
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
        curBuffer = dir.openFileReadOnly(getHashmapFilename(ver))
    }

    override fun close() {}
}

class SimpleHashMapEnv<K, V> private constructor(
        dir: VersionedDirectory,
        private val keySerializer: Serializer<K>,
        private val valueSerializer: Serializer<V>,
        private val bucketLayout: BucketLayout,
        private val numDataPages: Int,
        private val bucketsPerPage: Int
) : SimpleHashMapBaseEnv(dir) {
    class Builder<K, V>(keyClass: Class<K>, valueClass: Class<V>) {
        private val keySerializer: Serializer<K> = Serializer.getForClass(keyClass)
        private val valueSerializer: Serializer<V> = Serializer.getForClass(valueClass)
        private val bucketLayout: BucketLayout = BucketLayout(keySerializer.size, valueSerializer.size)

        private var capacity = -1
        private var numDataPages = -1
        private var bucketsPerPage = -1

        companion object {
            private const val VERSION_FILENAME = "hashmap.ver"
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
            val dir = VersionedDirectory.openWritable(path, VERSION_FILENAME)
            return if (dir.created) {
                create(dir)
            } else {
                openWritable(dir)
            }
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv<K, V> {
            val dir = VersionedDirectory.openReadOnly(path, VERSION_FILENAME)
            return SimpleHashMapROEnv(dir, keySerializer, valueSerializer, bucketLayout)
        }

        private fun create(dir: VersionedDirectory): SimpleHashMapEnv<K, V> {
            prepareCreate()
            println("buffer size: ${calcBufferSize(numDataPages)}")
            val version = dir.readVersion()
            val mapBuffer = dir.createFile(getHashmapFilename(version), calcBufferSize(numDataPages))
            val header = SimpleHashMap.Header(capacity, initialEntries)
            header.dump(mapBuffer)
            return SimpleHashMapEnv(dir, keySerializer, valueSerializer, bucketLayout, numDataPages, bucketsPerPage)
        }

        private fun openWritable(dir: VersionedDirectory): SimpleHashMapEnv<K, V> {
            return SimpleHashMapEnv(dir, keySerializer, valueSerializer, bucketLayout, numDataPages, bucketsPerPage)
        }
    }

    fun getMap(): SimpleHashMap<K, V> {
        val ver = dir.readVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return SimpleHashMapImpl(ver, mapBuffer, keySerializer, valueSerializer, bucketLayout, bucketsPerPage)
    }

    fun copyMap(map: SimpleHashMap<K, V>): SimpleHashMap<K, V> {
        val newVersion = map.version + 1
        // TODO Write into temporary file then rename
        val mapBuffer = dir.createFile(getHashmapFilename(newVersion), calcBufferSize(numDataPages))
        map.header.dump(mapBuffer)
        // TODO Really copy map data
        dir.writeVersion(newVersion)
        return getMap()
    }

    override fun close() = dir.close()
}
