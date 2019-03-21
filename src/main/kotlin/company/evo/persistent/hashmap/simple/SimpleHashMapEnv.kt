package company.evo.persistent.hashmap.simple

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock

import company.evo.persistent.FileDoesNotExistException
import company.evo.persistent.MappedFile
import company.evo.persistent.RefCounted
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
        collectStats: Boolean = false
) : SimpleHashMapBaseEnv(dir, collectStats) {

    private data class VersionedFile(
            val version: Long,
            val file: RefCounted<MappedFile>
    )

    private val lock = ReentrantLock()

    @Volatile
    private var currentFile: VersionedFile = openFile(dir)

    companion object {
        private fun openFile(dir: VersionedDirectory): VersionedFile {
            var version = dir.readVersion()
            while (true) {
                val newFile = tryOpenFile(dir, version)
                if (newFile != null) {
                    return newFile
                }
                val newVersion = dir.readVersion()
                if (newVersion == version) {
                    throw FileDoesNotExistException(Paths.get(getHashmapFilename(version)))
                }
                version = newVersion
            }
        }

        private fun tryOpenFile(dir: VersionedDirectory, version: Long): VersionedFile? {
            return try {
                VersionedFile(
                        version,
                        dir.openFileReadOnly(getHashmapFilename(version))
                )
            } catch (e: FileDoesNotExistException) {
                null
            }
        }
    }

    fun getCurrentMap(): SimpleHashMapRO_Int_Float {
        val curFile = currentFile
        val version = dir.readVersion()
        if (curFile.version != version) {
            if (lock.tryLock()) {
                try {
                    currentFile = openFile(dir)
                } finally {
                    lock.unlock()
                }
            }
        }

        return SimpleHashMapRO_Int_Float.create(curFile.version, curFile.file.acquire().buffer, collectStats)
    }

    override fun close() {}
}

class SimpleHashMapEnv_Int_Float private constructor(
        dir: VersionedDirectory,
        val loadFactor: Double,
        collectStats: Boolean = false
) : SimpleHashMapBaseEnv(dir, collectStats) {
    class Builder {
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

        var useUnmapHack: Boolean = false
        fun useUnmapHack(useUnmapHack: Boolean) = apply {
            this.useUnmapHack = useUnmapHack
        }

        fun open(path: Path): SimpleHashMapEnv_Int_Float {
            val dir = VersionedMmapDirectory.openWritable(path, VERSION_FILENAME)
            dir.useUnmapHack = useUnmapHack
            return if (dir.created) {
                create(dir)
            } else {
                openWritable(dir)
            }
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv_Int_Float {
            val dir = VersionedMmapDirectory.openReadOnly(path, VERSION_FILENAME)
            dir.useUnmapHack = useUnmapHack
            return SimpleHashMapROEnv_Int_Float(dir, collectStats)
        }

        fun createAnonymousDirect(): SimpleHashMapEnv_Int_Float {
            val dir = VersionedRamDirectory.createDirect()
            dir.useUnmapHack = useUnmapHack
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
            val mappedFile = dir.createFile(filename, mapInfo.bufferSize)
            val mappedBuffer = mappedFile.acquire().buffer
            SimpleHashMap_Int_Float.initBuffer(UnsafeBuffer(mappedBuffer), mapInfo)
            return SimpleHashMapEnv_Int_Float(dir, loadFactor, collectStats)
        }

        private fun openWritable(dir: VersionedDirectory): SimpleHashMapEnv_Int_Float {
            return SimpleHashMapEnv_Int_Float(dir, loadFactor, collectStats)
        }
    }

    fun openMap(): SimpleHashMap_Int_Float {
        val ver = dir.readVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return SimpleHashMap_Int_Float.create(ver, mapBuffer.acquire().buffer)
    }

    fun copyMap(map: SimpleHashMap_Int_Float): SimpleHashMap_Int_Float {
        val newVersion = map.version + 1
        val newMaxEntries = map.maxEntries * 2
        val mapInfo = MapInfo.calcFor(
                newMaxEntries, loadFactor, SimpleHashMap_Int_Float.bucketLayout.size
        )
        // TODO Write into temporary file then rename
        val mappedFile = dir.createFile(
                getHashmapFilename(newVersion), mapInfo.bufferSize
        )
        val mappedBuffer = mappedFile.acquire().buffer
        map.header.dump(mappedBuffer)
        // TODO Really copy map data
        val newMap = SimpleHashMap_Int_Float.create(newVersion, mappedBuffer)
        val iterator = map.iterator()
        while (iterator.next()) {
            newMap.put(iterator.key(), iterator.value())
        }

        dir.writeVersion(newVersion)
        dir.deleteFile(getHashmapFilename(map.version))
        return openMap()
    }

    override fun close() {
        dir.close()
    }
}
