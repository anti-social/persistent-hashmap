package dev.evo.persistent.hashmap.straight

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock

import kotlin.random.Random

import dev.evo.io.IOBuffer
import dev.evo.persistent.FileDoesNotExistException
import dev.evo.persistent.MappedFile
import dev.evo.persistent.VersionedDirectory
import dev.evo.persistent.VersionedMmapDirectory
import dev.evo.persistent.VersionedRamDirectory
import dev.evo.persistent.hashmap.Hasher
import dev.evo.persistent.BufferManagement
import dev.evo.rc.RefCounted
import dev.evo.rc.use

abstract class StraightHashMapBaseEnv protected constructor(
        protected val dir: VersionedDirectory
) : AutoCloseable
{
    companion object {
        fun getHashmapFilename(version: Long) = "hashmap_$version.data"
    }

    fun getCurrentVersion() = dir.readVersion()
}

class StraightHashMapROEnv<H: Hasher, W: StraightHashMap, RO: StraightHashMapRO> (
        dir: VersionedDirectory,
        private val mapType: StraightHashMapType<H, W, RO>
) : StraightHashMapBaseEnv(dir) {

    private data class VersionedFile(
            val version: Long,
            val file: RefCounted<MappedFile<IOBuffer>>
    )

    private val lock = ReentrantLock()

    @Volatile
    private var currentFile: VersionedFile? = null

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

        fun readMapHeader(dir: VersionedDirectory): Header {
            val mapFile = openFile(dir)
            try {
                return Header.load(mapFile.file.get().buffer)
            } finally {
                mapFile.file.release()
            }
        }
    }

    fun getCurrentMap(): RO {
        var curFile: VersionedFile
        while (true) {
            curFile = currentFile ?: lock.lock().let {
                // Another thread could update the current file so check it again
                currentFile ?: try {
                    openFile(dir)
                } finally {
                    lock.unlock()
                }
            }
            // Try to retain the map file.
            // It might fail if other readers closed it
            if (curFile.file.retain() != null) {
                break
            }
        }

        val version = dir.readVersion()
        if (curFile.version != version) {
            if (lock.tryLock()) {
                try {
                    val newFile = openFile(dir)
                    currentFile = newFile
                    // Release an old map file
                    curFile.file.release()
                    curFile = newFile
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
        return mapType.createReadOnly(curFile.version, curFile.file)
    }

    override fun close() {
        currentFile?.let { curFile ->
            curFile.file.release()
        }
        dir.close()
    }
}

class StraightHashMapEnv<H: Hasher, W: StraightHashMap, RO: StraightHashMapRO> private constructor(
        dir: VersionedDirectory,
        val loadFactor: Double,
        private val mapType: StraightHashMapType<H, W, RO>,
        private val hasher: Hasher
) : StraightHashMapBaseEnv(dir) {

    class Builder<H: Hasher, W: StraightHashMap, RO: StraightHashMapRO>(
            private val mapType: StraightHashMapType<H, W, RO>
    ) {
        companion object {
            private const val DEFAULT_INITIAL_ENTRIES = 1024
            private const val DEFAULT_LOAD_FACTOR = 0.75
        }

        var hasher: Hasher = mapType.hasherProvider.run {
            getHasher(defaultHasherSerial)
        }
        fun hasher(serial: Long) = apply {
            hasher = mapType.hasherProvider.run {
                getHasher(serial)
            }
        }
        fun hasher(hasher: H) = apply {
            this.hasher = hasher
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

        var maxDistance: Int = 0
        fun maxDistance(maxDist: Int) = apply {
            this.maxDistance = maxDist
        }

        var bufferManagement: BufferManagement = BufferManagement.MemorySegments
        fun bufferManagement(bufferManagement: BufferManagement) = apply {
            this.bufferManagement = bufferManagement
        }

        fun open(path: Path): StraightHashMapEnv<H, W, RO> {
            val dir = VersionedMmapDirectory.openWritable(
                path, VERSION_FILENAME, bufferManagement
            )
            if (dir.created) {
                initialize(dir)
            }
            return openWritable(dir)
        }

        fun openReadOnly(path: Path): StraightHashMapROEnv<H, W, RO> {
            val dir = VersionedMmapDirectory.openReadOnly(
                path, VERSION_FILENAME, bufferManagement
            )
            return StraightHashMapROEnv(dir, mapType)
        }

        fun createAnonymousDirect(): StraightHashMapEnv<H, W, RO> {
            val dir = VersionedRamDirectory.createDirect(bufferManagement)
            initialize(dir)
            return openWritable(dir)
        }

        fun createAnonymousHeap(): StraightHashMapEnv<H, W, RO> {
            val dir = VersionedRamDirectory.createHeap()
            initialize(dir)
            return openWritable(dir)
        }

        private fun initialize(dir: VersionedDirectory) {
            val version = dir.readVersion()
            val filename = getHashmapFilename(version)
            val mapInfo = MapInfo.calcFor(
                    initialEntries, loadFactor, mapType.bucketLayout.size,  maxDistance
            )
            dir.createFile(filename, mapInfo.bufferSize).use { file ->
                mapInfo.initBuffer(
                        file.buffer,
                        mapType.keySerializer,
                        mapType.valueSerializer,
                        hasher
                )
            }
        }

        private fun openWritable(dir: VersionedDirectory): StraightHashMapEnv<H, W, RO> {
            return StraightHashMapEnv(dir, loadFactor, mapType, hasher)
        }
    }

    companion object {
        const val VERSION_FILENAME = "hashmap.ver"

        private val TEMP_SYMBOLS = ('0'..'9').toList() + ('a'..'z').toList() + ('A'..'Z').toList()

        private fun tempFileName(): String {
            val randomPart = (1..8).fold("") { s, _ ->
                s + TEMP_SYMBOLS[Random.nextInt(TEMP_SYMBOLS.size)]
            }
            return ".hashmap_$randomPart.tmp"
        }
    }

    fun openMap(): W {
        val ver = dir.readVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return mapType.createWritable(ver, mapBuffer)
    }

    fun newMap(oldMap: W, maxEntries: Int): W {
        val version = oldMap.version + 1
        val bookmarks = oldMap.loadAllBookmarks()
        val mapInfo = MapInfo.calcFor(
                maxEntries, loadFactor, mapType.bucketLayout.size, oldMap.maxDistance
        )
        val mapFilename = tempFileName()
        val mappedFile = dir.createFile(
                mapFilename, mapInfo.bufferSize, deleteOnExit = true
        )
        mapInfo.initBuffer(
                mappedFile.get().buffer,
                mapType.keySerializer,
                mapType.valueSerializer,
                hasher
        )
        return mapType.createWritable(version, mappedFile).apply {
            storeAllBookmarks(bookmarks)
        }
    }

    fun copyMap(map: W): W {
        var newMaxEntries = map.size() * 2
        while (true) {
            val newMap = newMap(map, newMaxEntries)
            if (!mapType.copyMap(map, newMap)) {
                // Too many collisions, double number of maximum entries
                newMaxEntries *= 2
                newMap.close()
                continue
            } else {
                return newMap
            }
        }
    }

    fun commit(map: W) {
        val curVersion = dir.readVersion()
        if (map.version <= curVersion) {
            throw IllegalArgumentException("Map has already been committed")
        }
        dir.rename(map.name, getHashmapFilename(map.version))
        map.flush()
        dir.writeVersion(map.version)
        dir.deleteFile(getHashmapFilename(curVersion))
    }

    fun discard(map: W) {
        val curVersion = dir.readVersion()
        if (map.version == curVersion) {
            throw IllegalArgumentException("Cannot delete active map")
        }
        dir.deleteFile(map.name)
    }

    override fun close() {
        dir.close()
    }
}
