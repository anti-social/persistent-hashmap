package company.evo.persistent.hashmap.simple

import company.evo.persistent.FileDoesNotExistException
import company.evo.persistent.VersionedDirectory
import company.evo.persistent.VersionedMmapDirectory
import company.evo.persistent.VersionedRamDirectory
import company.evo.persistent.hashmap.BucketLayout
import company.evo.persistent.hashmap.PRIMES
import company.evo.persistent.hashmap.Serializer

import java.nio.ByteBuffer
import java.nio.file.Path
import java.nio.file.Paths

import java.util.concurrent.locks.ReentrantLock

abstract class SimpleHashMapBaseEnv(
        protected val dir: VersionedDirectory
) : AutoCloseable
{
    companion object {
        const val MAX_RETRIES = 1000
        const val MAX_DISTANCE = 1024

        fun getHashmapFilename(version: Long) = "hashmap_$version.data"
    }

    fun getCurrentVersion() = dir.readVersion()
}

class SimpleHashMapROEnv<K, V> (
        dir: VersionedDirectory,
        val bucketLayout: BucketLayout<K, V>
) : SimpleHashMapBaseEnv(dir) {

    private val lock = ReentrantLock()

    @Volatile
    private var curVersion: Long = 0

    @Volatile
    private var curBuffer: ByteBuffer = dir.openFileReadOnly(getHashmapFilename(dir.readVersion()))

    fun currentMap(): SimpleHashMapRO<K, V> {
        var ver = dir.readVersion()
        var i = 0
        while (ver != curVersion) {
            if (lock.tryLock()) {
                try {
                    if (refresh(ver)) {
                        break
                    }
                } finally {
                    lock.unlock()
                }
            }
            val newVer = dir.readVersion()
            if (newVer == ver) {
                throw FileDoesNotExistException(Paths.get(getHashmapFilename(ver)))
            }
            ver = newVer
            i++
        }

        return SimpleHashMapRO.fromEnv(
                this,
                curBuffer
                        .duplicate()
                        .clear()
                        .order(VersionedDirectory.BYTE_ORDER)
        )
    }

    private fun refresh(ver: Long): Boolean {
        try {
            val mapBuffer = dir.openFileReadOnly(getHashmapFilename(ver))
            curVersion = ver
            curBuffer = mapBuffer
        } catch (e: FileDoesNotExistException) {
            return false
        }
        return true
    }

    override fun close() {}
}

class SimpleHashMapEnv<K, V> private constructor(
        dir: VersionedDirectory,
        val bucketLayout: BucketLayout<K, V>,
        val loadFactor: Double
) : SimpleHashMapBaseEnv(dir) {
    class Builder<K, V>(keyClass: Class<K>, valueClass: Class<V>) {
        private val keySerializer: Serializer<K> = Serializer.getForClass(keyClass)
        private val valueSerializer: Serializer<V> = Serializer.getForClass(valueClass)
        private val bucketLayout = BucketLayout(keySerializer, valueSerializer, SimpleHashMap.META_SIZE)

        companion object {
            private const val VERSION_FILENAME = "hashmap.ver"
            private const val DEFAULT_LOAD_FACTOR = 0.75

            inline operator fun <reified K, reified V> invoke(): Builder<K, V> {
                return Builder(K::class.java, V::class.java)
            }
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

        fun open(path: Path): SimpleHashMapEnv<K, V> {
            val dir = VersionedMmapDirectory.openWritable(path, VERSION_FILENAME)
            return if (dir.created) {
                create(dir)
            } else {
                openWritable(dir)
            }
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv<K, V> {
            val dir = VersionedMmapDirectory.openReadOnly(path, VERSION_FILENAME)
            return SimpleHashMapROEnv(dir, bucketLayout)
        }

        fun createAnonymousDirect(): SimpleHashMapEnv<K, V> {
            val dir = VersionedRamDirectory.createDirect()
            return create(dir)
        }

        fun createAnonymousHeap(): SimpleHashMapEnv<K, V> {
            val dir = VersionedRamDirectory.createHeap()
            return create(dir)
        }

        private fun create(dir: VersionedDirectory): SimpleHashMapEnv<K, V> {
            val version = dir.readVersion()
            val filename = getHashmapFilename(version)
            val mapInfo = MapInfo.calcFor(initialEntries, loadFactor, bucketLayout.size)
            val mapBuffer = dir.createFile(filename, mapInfo.bufferSize)
            SimpleHashMap.initBuffer(mapBuffer, bucketLayout, mapInfo)
            return SimpleHashMapEnv(dir, bucketLayout, loadFactor)
        }

        private fun openWritable(dir: VersionedDirectory): SimpleHashMapEnv<K, V> {
            return SimpleHashMapEnv(dir, bucketLayout, loadFactor)
        }
    }

    fun openMap(): SimpleHashMap<K, V> {
        val ver = getCurrentVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return SimpleHashMap.fromEnv(this, mapBuffer)
    }

    fun copyMap(map: SimpleHashMap<K, V>): SimpleHashMap<K, V> {
        val newVersion = map.version + 1
        val newMaxEntries = map.maxEntries * 2
        val mapInfo = MapInfo.calcFor(newMaxEntries, loadFactor, bucketLayout.size)
        // TODO Write into temporary file then rename
        val mapBuffer = dir.createFile(
                getHashmapFilename(newVersion), mapInfo.bufferSize
        )
        map.header.dump(mapBuffer)
        // TODO Really copy map data
        dir.writeVersion(newVersion)
        dir.deleteFile(getHashmapFilename(map.version))
        return openMap()
    }

    override fun close() = dir.close()
}
