package company.evo.persistent.hashmap.simple

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock

import company.evo.persistent.FileDoesNotExistException
import company.evo.persistent.VersionedDirectory
import company.evo.persistent.VersionedMmapDirectory
import company.evo.persistent.VersionedRamDirectory
import company.evo.persistent.hashmap.PRIMES
import org.agrona.concurrent.AtomicBuffer
import org.agrona.concurrent.UnsafeBuffer

abstract class SimpleHashMapBaseEnv(
        protected val dir: VersionedDirectory,
        val collectStats: Boolean
) : AutoCloseable
{
    companion object {
        const val MAX_DISTANCE = 1024

        fun getHashmapFilename(version: Long) = "hashmap_$version.data"
    }

    fun getCurrentVersion() = dir.readVersion()
}

class SimpleHashMapROEnv_Int_Float (
        dir: VersionedDirectory,
        collectStats: Boolean
) : SimpleHashMapBaseEnv(dir, collectStats) {

    private val lock = ReentrantLock()

    @Volatile
    private var curVersion: Long = 0

    @Volatile
    private var curBuffer: AtomicBuffer = dir.openFileReadOnly(getHashmapFilename(dir.readVersion()))

    fun currentMap(): SimpleHashMapRO_Int_Float {
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

        return SimpleHashMapRO_Int_Float.fromEnv(this, curBuffer)
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

class SimpleHashMapEnv_Int_Float private constructor(
        dir: VersionedDirectory,
        val loadFactor: Double,
        collectStats: Boolean
) : SimpleHashMapBaseEnv(dir, collectStats) {
    class Builder {
        private val keySerializer = Serializer_K()
        private val valueSerializer = Serializer_V()

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

        fun open(path: Path): SimpleHashMapEnv_Int_Float {
            val dir = VersionedMmapDirectory.openWritable(path, VERSION_FILENAME)
            return if (dir.created) {
                create(dir)
            } else {
                openWritable(dir)
            }
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv_Int_Float {
            val dir = VersionedMmapDirectory.openReadOnly(path, VERSION_FILENAME)
            return SimpleHashMapROEnv_Int_Float(dir, collectStats)
        }

        fun createAnonymousDirect(): SimpleHashMapEnv_Int_Float {
            val dir = VersionedRamDirectory.createDirect()
            return create(dir)
        }

        fun createAnonymousHeap(): SimpleHashMapEnv_Int_Float {
            val dir = VersionedRamDirectory.createHeap()
            return create(dir)
        }

        private fun create(dir: VersionedDirectory): SimpleHashMapEnv_Int_Float {
            val version = dir.readVersion()
            val filename = getHashmapFilename(version)
            val mapInfo = MapInfo.calcFor(
                    initialEntries, loadFactor, SimpleHashMap_Int_Float.bucketLayout.size
            )
            val mapBuffer = dir.createFile(filename, mapInfo.bufferSize)
            SimpleHashMap_Int_Float.initBuffer(UnsafeBuffer(mapBuffer), mapInfo)
            return SimpleHashMapEnv_Int_Float(dir, loadFactor, collectStats)
        }

        private fun openWritable(dir: VersionedDirectory): SimpleHashMapEnv_Int_Float {
            return SimpleHashMapEnv_Int_Float(dir, loadFactor, collectStats)
        }
    }

    fun openMap(): SimpleHashMap_Int_Float {
        val ver = getCurrentVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return SimpleHashMap_Int_Float.fromEnv(this, UnsafeBuffer(mapBuffer))
    }

    fun copyMap(map: SimpleHashMap_Int_Float): SimpleHashMap_Int_Float {
        val newVersion = map.version + 1
        val newMaxEntries = map.maxEntries * 2
        val mapInfo = MapInfo.calcFor(
                newMaxEntries, loadFactor, SimpleHashMap_Int_Float.bucketLayout.size
        )
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
