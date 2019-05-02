package company.evo.persistent.hashmap.straight

import company.evo.io.IOBuffer
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock

import company.evo.persistent.FileDoesNotExistException
import company.evo.persistent.MappedFile
import company.evo.persistent.VersionedDirectory
import company.evo.persistent.VersionedMmapDirectory
import company.evo.persistent.VersionedRamDirectory
import company.evo.persistent.hashmap.Hasher
import company.evo.rc.RefCounted
import company.evo.rc.use

abstract class StraightHashMapBaseEnv protected constructor(
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

class StraightHashMapROEnv<K, V, W: StraightHashMap, RO: StraightHashMap> (
        dir: VersionedDirectory,
        private val mapType: StraightHashMapType<K, V, W, RO>,
        collectStats: Boolean = false
) : StraightHashMapBaseEnv(dir, collectStats) {

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
        return mapType.createReadOnly(curFile.version, curFile.file, collectStats)
    }

    override fun close() {
        currentFile.file.release()
        dir.close()
    }
}

class StraightHashMapEnv<K, V, W: StraightHashMap, RO: StraightHashMap> private constructor(
        dir: VersionedDirectory,
        val loadFactor: Double,
        private val mapType: StraightHashMapType<K, V, W, RO>,
        private val hasher: Hasher<K>,
        collectStats: Boolean = false
) : StraightHashMapBaseEnv(dir, collectStats) {

    class Builder<K, V, W: StraightHashMap, RO: StraightHashMap>(private val mapType: StraightHashMapType<K, V, W, RO>) {
        companion object {
            private const val VERSION_FILENAME = "hashmap.ver"
            private const val DEFAULT_INITIAL_ENTRIES = 1024
            private const val DEFAULT_LOAD_FACTOR = 0.75
        }

        var hasher: Hasher<K> = mapType.hasherProvider.run {
            getHasher(defaultHasherSerial)
        }
        fun hasher(serial: Long) = apply {
            hasher = mapType.hasherProvider.run {
                getHasher(serial)
            }
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

        fun open(path: Path): StraightHashMapEnv<K, V, W, RO> {
            val dir = VersionedMmapDirectory.openWritable(path, VERSION_FILENAME)
            dir.useUnmapHack = useUnmapHack
            return if (dir.created) {
                create(dir)
            } else {
                openWritable(dir)
            }
        }

        fun openReadOnly(path: Path): StraightHashMapROEnv<K, V, W, RO> {
            val dir = VersionedMmapDirectory.openReadOnly(path, VERSION_FILENAME)
            dir.useUnmapHack = useUnmapHack
            return StraightHashMapROEnv(dir, mapType, collectStats)
        }

        fun createAnonymousDirect(): StraightHashMapEnv<K, V, W, RO> {
            val dir = VersionedRamDirectory.createDirect()
            dir.useUnmapHack = useUnmapHack
            return create(dir)
        }

        fun createAnonymousHeap(): StraightHashMapEnv<K, V, W, RO> {
            val dir = VersionedRamDirectory.createHeap()
            return create(dir)
        }

        private fun create(dir: VersionedDirectory): StraightHashMapEnv<K, V, W, RO> {
            val version = dir.readVersion()
            val filename = getHashmapFilename(version)
            val mapInfo = MapInfo.calcFor(
                    initialEntries, loadFactor, mapType.bucketLayout.size
            )
            dir.createFile(filename, mapInfo.bufferSize).use { file ->
                mapInfo.initBuffer(
                        file.buffer,
                        mapType.keySerializer,
                        mapType.valueSerializer,
                        hasher
                )
            }
            return StraightHashMapEnv(dir, loadFactor, mapType, hasher, collectStats)
        }

        private fun openWritable(dir: VersionedDirectory): StraightHashMapEnv<K, V, W, RO> {
            return StraightHashMapEnv(dir, loadFactor, mapType, hasher, collectStats)
        }
    }

    fun openMap(): W {
        val ver = dir.readVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return mapType.createWritable(ver, mapBuffer)
    }

    fun copyMap(map: W): W {
        val newVersion = map.version + 1
        var newMaxEntries = map.size() * 2
        while (true) {
            val newMapInfo = MapInfo.calcFor(
                    newMaxEntries, loadFactor, mapType.bucketLayout.size
            )
            // TODO Write into temporary file then rename
            val newMapFilename = getHashmapFilename(newVersion)
            val newMappedFile = dir.createFile(
                    newMapFilename, newMapInfo.bufferSize
            )
            val newMappedBuffer = newMappedFile.get().buffer
            newMapInfo.initBuffer(
                    newMappedBuffer,
                    mapType.keySerializer,
                    mapType.valueSerializer,
                    hasher
            )
            if (!mapType.createWritable(newVersion, newMappedFile).use { newMap ->
                if (!mapType.copyMap(map, newMap)) {
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
