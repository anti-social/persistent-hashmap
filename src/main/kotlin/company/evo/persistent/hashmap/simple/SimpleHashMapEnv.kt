package company.evo.persistent.hashmap.simple

import java.nio.ByteBuffer
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock

import company.evo.persistent.FileDoesNotExistException
import company.evo.persistent.VersionedDirectory
import company.evo.persistent.VersionedMmapDirectory
import company.evo.persistent.VersionedRamDirectory
import company.evo.persistent.hashmap.BucketLayout_K_V
import company.evo.persistent.hashmap.PRIMES
import company.evo.persistent.hashmap.Serializer_K
import company.evo.persistent.hashmap.Serializer_V
import org.agrona.concurrent.UnsafeBuffer

abstract class SimpleHashMapBaseEnv(
        protected val dir: VersionedDirectory,
        val collectStats: Boolean
) : AutoCloseable
{
    companion object {
        const val MAX_RETRIES = 1000
        const val MAX_DISTANCE = 1024

        fun getHashmapFilename(version: Long) = "hashmap_$version.data"
    }

    fun getCurrentVersion() = dir.readVersion()
}

class SimpleHashMapROEnv_K_V (
        dir: VersionedDirectory,
        val bucketLayout: BucketLayout_K_V,
        collectStats: Boolean
) : SimpleHashMapBaseEnv(dir, collectStats) {

    private val lock = ReentrantLock()

    @Volatile
    private var curVersion: Long = 0

    @Volatile
    private var curBuffer: ByteBuffer = dir.openFileReadOnly(getHashmapFilename(dir.readVersion()))

    fun currentMap(): SimpleHashMapRO_K_V {
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

        return SimpleHashMapRO_K_V.fromEnv(
                this,
                UnsafeBuffer(curBuffer
                        .duplicate()
                        .clear()
                        .order(VersionedDirectory.BYTE_ORDER)
                )
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

class SimpleHashMapEnv_K_V private constructor(
        dir: VersionedDirectory,
        val bucketLayout: BucketLayout_K_V,
        val loadFactor: Double,
        collectStats: Boolean
) : SimpleHashMapBaseEnv(dir, collectStats) {
    class Builder() {
        private val keySerializer = Serializer_K()
        private val valueSerializer = Serializer_V()
        private val bucketLayout = BucketLayout_K_V(keySerializer, valueSerializer, SimpleHashMap_K_V.META_SIZE)

        companion object {
            private const val VERSION_FILENAME = "hashmap.ver"
            private const val DEFAULT_LOAD_FACTOR = 0.75

            operator fun invoke(): Builder {
                return Builder()
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

        var collectStats: Boolean = false
        fun collectStats(collectStats: Boolean) = apply {
            this.collectStats = collectStats
        }

        fun open(path: Path): SimpleHashMapEnv_K_V {
            val dir = VersionedMmapDirectory.openWritable(path, VERSION_FILENAME)
            return if (dir.created) {
                create(dir)
            } else {
                openWritable(dir)
            }
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv_K_V {
            val dir = VersionedMmapDirectory.openReadOnly(path, VERSION_FILENAME)
            return SimpleHashMapROEnv_K_V(dir, bucketLayout, collectStats)
        }

        fun createAnonymousDirect(): SimpleHashMapEnv_K_V {
            val dir = VersionedRamDirectory.createDirect()
            return create(dir)
        }

        fun createAnonymousHeap(): SimpleHashMapEnv_K_V {
            val dir = VersionedRamDirectory.createHeap()
            return create(dir)
        }

        private fun create(dir: VersionedDirectory): SimpleHashMapEnv_K_V {
            val version = dir.readVersion()
            val filename = getHashmapFilename(version)
            val mapInfo = MapInfo.calcFor(initialEntries, loadFactor, bucketLayout.size)
            val mapBuffer = dir.createFile(filename, mapInfo.bufferSize)
            SimpleHashMap_K_V.initBuffer(UnsafeBuffer(mapBuffer), bucketLayout, mapInfo)
            return SimpleHashMapEnv_K_V(dir, bucketLayout, loadFactor, collectStats)
        }

        private fun openWritable(dir: VersionedDirectory): SimpleHashMapEnv_K_V {
            return SimpleHashMapEnv_K_V(dir, bucketLayout, loadFactor, collectStats)
        }
    }

    fun openMap(): SimpleHashMap_K_V {
        val ver = getCurrentVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return SimpleHashMap_K_V.fromEnv(this, UnsafeBuffer(mapBuffer))
    }

    fun copyMap(map: SimpleHashMap_K_V): SimpleHashMap_K_V {
        val newVersion = map.version + 1
        val newMaxEntries = map.maxEntries * 2
        val mapInfo = MapInfo.calcFor(newMaxEntries, loadFactor, bucketLayout.size)
        // TODO Write into temporary file then rename
        val mapBuffer = dir.createFile(
                getHashmapFilename(newVersion), mapInfo.bufferSize
        )
        map.header.dump(UnsafeBuffer(mapBuffer))
        // TODO Really copy map data
        dir.writeVersion(newVersion)
        dir.deleteFile(getHashmapFilename(map.version))
        return openMap()
    }

    override fun close() = dir.close()
}
