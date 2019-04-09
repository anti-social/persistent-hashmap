package company.evo.persistent.hashmap.simple

import company.evo.io.IOBuffer
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock

import company.evo.persistent.FileDoesNotExistException
import company.evo.persistent.MappedFile
import company.evo.persistent.VersionedDirectory
import company.evo.persistent.VersionedMmapDirectory
import company.evo.persistent.VersionedRamDirectory
import company.evo.rc.RefCounted
import company.evo.rc.use

abstract class SimpleHashMapBaseEnv protected constructor(
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

class SimpleHashMapROEnv<K, V, W: SimpleHashMap, RO: SimpleHashMap> (
        dir: VersionedDirectory,
        protected val mapProvider: SimpleHashMapProvider<K, V, W, RO>,
        collectStats: Boolean = false
) : SimpleHashMapBaseEnv(dir, collectStats) {

    private data class VersionedFile(
            val version: Long,
            val file: RefCounted<MappedFile<IOBuffer>>
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

    fun getCurrentMap(): RO {
        var curFile: VersionedFile
        // Retain a map file
        while (true) {
            curFile = currentFile
            if (curFile.file.retain() != null) {
                break
            }
        }

        val version = dir.readVersion()
        if (curFile.version != version) {
            if (lock.tryLock()) {
                try {
                    currentFile = openFile(dir)
                    // Release an old map file
                    curFile.file.release()
                    curFile = currentFile
                    // Retain a map file
                    // we just now created the map file and we are under a lock
                    // so calling retain should be always successful
                    curFile.file.retain() ?:
                            throw IllegalStateException("Somehow the file just opened has been released")
                } finally {
                    lock.unlock()
                }
            }
        }

        // File will be released when closing a hash map
        return mapProvider.createReadOnly(curFile.version, curFile.file, collectStats)
    }

    override fun close() {
        currentFile.file.release()
        dir.close()
    }
}

class SimpleHashMapEnv<K, V, W: SimpleHashMap, RO: SimpleHashMap> private constructor(
        dir: VersionedDirectory,
        val loadFactor: Double,
        private val mapProvider: SimpleHashMapProvider<K, V, W, RO>,
        collectStats: Boolean = false
) : SimpleHashMapBaseEnv(dir, collectStats) {

    class Builder<K, V, W: SimpleHashMap, RO: SimpleHashMap>(private val mapProvider: SimpleHashMapProvider<K, V, W, RO>) {
        companion object {
            private const val VERSION_FILENAME = "hashmap.ver"
            private const val DEFAULT_INITIAL_ENTRIES = 1024
            private const val DEFAULT_LOAD_FACTOR = 0.75
        }

        var initialEntries: Int = DEFAULT_INITIAL_ENTRIES
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

        fun open(path: Path): SimpleHashMapEnv<K, V, W, RO> {
            val dir = VersionedMmapDirectory.openWritable(path, VERSION_FILENAME)
            dir.useUnmapHack = useUnmapHack
            return if (dir.created) {
                create(dir)
            } else {
                openWritable(dir)
            }
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv<K, V, W, RO> {
            val dir = VersionedMmapDirectory.openReadOnly(path, VERSION_FILENAME)
            dir.useUnmapHack = useUnmapHack
            return SimpleHashMapROEnv(dir, mapProvider, collectStats)
        }

        fun createAnonymousDirect(): SimpleHashMapEnv<K, V, W, RO> {
            val dir = VersionedRamDirectory.createDirect()
            dir.useUnmapHack = useUnmapHack
            return create(dir)
        }

        fun createAnonymousHeap(): SimpleHashMapEnv<K, V, W, RO> {
            val dir = VersionedRamDirectory.createHeap()
            return create(dir)
        }

        private fun create(dir: VersionedDirectory): SimpleHashMapEnv<K, V, W, RO> {
            val version = dir.readVersion()
            val filename = getHashmapFilename(version)
            val mapInfo = MapInfo.calcFor(
                    initialEntries, loadFactor, mapProvider.bucketLayout.size
            )
            dir.createFile(filename, mapInfo.bufferSize).use { file ->
                mapInfo.initBuffer(
                        file.buffer,
                        mapProvider.keySerializer,
                        mapProvider.valueSerializer
                )
            }
            return SimpleHashMapEnv(dir, loadFactor, mapProvider, collectStats)
        }

        private fun openWritable(dir: VersionedDirectory): SimpleHashMapEnv<K, V, W, RO> {
            return SimpleHashMapEnv(dir, loadFactor, mapProvider, collectStats)
        }
    }

    fun openMap(): W {
        val ver = dir.readVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return mapProvider.createWritable(ver, mapBuffer)
    }

    fun copyMap(map: W): W {
        val newVersion = map.version + 1
        var newMaxEntries = map.size() * 2
        while (true) {
            val newMapInfo = MapInfo.calcFor(
                    newMaxEntries, loadFactor, mapProvider.bucketLayout.size
            )
            // TODO Write into temporary file then rename
            val newMapFilename = getHashmapFilename(newVersion)
            val newMappedFile = dir.createFile(
                    newMapFilename, newMapInfo.bufferSize
            )
            val newMappedBuffer = newMappedFile.get().buffer
            newMapInfo.initBuffer(
                    newMappedBuffer,
                    mapProvider.keySerializer,
                    mapProvider.valueSerializer
            )
            if (!mapProvider.createWritable(newVersion, newMappedFile).use { newMap ->
                if (!mapProvider.copyMap(map, newMap)) {
                    newMaxEntries *= 2
                    dir.deleteFile(newMapFilename)
                    false
                } else {
                    true
                }
            }) continue
            break
        }
        dir.writeVersion(newVersion)

        dir.deleteFile(getHashmapFilename(map.version))

        return openMap()
    }

    override fun close() {
        dir.close()
    }
}
