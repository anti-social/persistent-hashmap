package company.evo.persistent.hashmap

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantLock

open class PersistentHashMapException(msg: String, cause: Exception? = null) : Exception(msg, cause)
class WriteLockException(msg: String, cause: Exception? = null) : PersistentHashMapException(msg, cause)
class CorruptedVersionFileException(msg: String) : PersistentHashMapException(msg)

interface SimpleHashMapRO<K, V> {
    val version: Long
    fun get(key: K): V
}

interface SimpleHashMap<K, V> : SimpleHashMapRO<K, V> {
    fun put(key: K, value: V): Boolean
    fun remove(key: K): Boolean
}

open class SimpleHashMapROImpl<K, V>(
        override val version: Long,
        private val buffer: ByteBuffer
) : SimpleHashMapRO<K, V> {
    override fun get(key: K): V {
        TODO("not implemented")
    }
}

class SimpleHashMapImpl<K, V>(
        version: Long,
        private val buffer: ByteBuffer
) : SimpleHashMap<K, V>, SimpleHashMapROImpl<K, V>(version, buffer) {
    override fun put(key: K, value: V): Boolean {
        TODO("not implemented")
    }

    override fun remove(key: K): Boolean {
        TODO("not implemented")
    }
}

abstract class SimpleHashMapBaseEnv(
        protected val mapDir: MapDirectory
) : AutoCloseable
{
    companion object {
        const val MAX_RETRIES = 100
    }

    fun getCurrentVersion() = mapDir.readVersion()
}

class SimpleHashMapROEnv<K, V>(
        mapDir: MapDirectory
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

        return SimpleHashMapROImpl(ver, curBuffer.duplicate())
    }

    private fun refresh(ver: Long) {
        curVersion = ver
        curBuffer = mapDir.openMap(ver)
    }

    override fun close() {}
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
                        val channel = file.channel
                        channel.map(mode.mapMode(), 0, channel.size())
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

    fun openMap(version: Long): ByteBuffer {
        return RandomAccessFile(getMapPath(path, version).toString(), mode.mode()).use { file ->
            val channel = file.channel
            channel.map(mode.mapMode(), 0, channel.size())
        }
    }
}

class SimpleHashMapEnv<K, V>(
        mapDir: MapDirectory,
        private val versionFileLock: FileLock
) : SimpleHashMapBaseEnv(mapDir) {
    class Builder<K, V> {
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
            return SimpleHashMapROEnv(mapDir)
        }

        private fun create(path: Path): SimpleHashMapEnv<K, V> {
            val mapDir = MapDirectory(path, Mode.CREATE)
            val verLock = mapDir.acquireLock()
            val verBuffer = mapDir.writeVersion(0L)
            mapDir.openMap(0L)
            return SimpleHashMapEnv(mapDir, verLock)
        }

        private fun openWritable(path: Path): SimpleHashMapEnv<K, V> {
            val mapDir = MapDirectory(path, Mode.OPEN_RW)
            val verLock = mapDir.acquireLock()
            return SimpleHashMapEnv(mapDir, verLock)
        }

    }

    fun getMap(): SimpleHashMap<K, V> {
        val ver = mapDir.readVersion()
        val mapBuffer = mapDir.openMap(ver)
        return SimpleHashMapImpl(ver, mapBuffer)
    }

    override fun close() {
        versionFileLock.release()
    }
}
