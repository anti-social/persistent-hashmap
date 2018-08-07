package company.evo.persistent.hashmap

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path

open class PersistentHashMapException(msg: String, cause: Exception? = null) : Exception(msg, cause)
class WriteLockException(msg: String, cause: Exception? = null) : PersistentHashMapException(msg, cause)
class CorruptedVersionFileException(msg: String) : PersistentHashMapException(msg)

interface SimpleHashMapRO<K, V> {
    fun get(key: K): V
}

interface SimpleHashMap<K, V> : SimpleHashMapRO<K, V> {
    fun put(key: K, value: V): Boolean
    fun remove(key: K): Boolean
}

open class SimpleHashMapROImpl<K, V>(
        private val buffer: ByteBuffer
) : SimpleHashMapRO<K, V> {
    override fun get(key: K): V {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class SimpleHashMapImpl<K, V>(
        private val buffer: ByteBuffer
) : SimpleHashMap<K, V>, SimpleHashMapROImpl<K, V>(buffer) {
    override fun put(key: K, value: V): Boolean {
        TODO("not implemented")
    }

    override fun remove(key: K): Boolean {
        TODO("not implemented")
    }
}

abstract class SimpleHashMapBaseEnv(
        protected val path: Path,
        private val versionBuffer: ByteBuffer
) : AutoCloseable
{
    companion object {
        const val MAX_RETRIES = 100
    }

    fun getVersion() = versionBuffer.getLong(0)

    protected fun getMapFilename(version: Long) = "hashmap_$version.data"

    protected fun getMapPath(version: Long) = path.resolve(getMapFilename(version))
}

class SimpleHashMapROEnv<K, V>(
        path: Path,
        versionBuffer: ByteBuffer
) : SimpleHashMapBaseEnv(path, versionBuffer) {
    fun getMap(): SimpleHashMapRO<K, V> {
        val mapPath = getMapPath(getVersion())
        val mapBuffer = RandomAccessFile(mapPath.toString(), "r").use { file ->
            val channel = file.channel
            channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
        }
        return SimpleHashMapROImpl(mapBuffer)
    }

    override fun close() {}
}

class SimpleHashMapEnv<K, V>(
        path: Path,
        versionBuffer: ByteBuffer,
        private val versionFileLock: FileLock
) : SimpleHashMapBaseEnv(path, versionBuffer) {
    class Builder<K, V> {
        companion object {
            const val VERSION_FILENAME = "hashmap.ver"
            const val VERSION_LENGTH = 8L
        }

        private enum class Mode {
            CREATE, OPEN_RO, OPEN_RW;

            fun mode() = when (this) {
                CREATE, OPEN_RW -> "rw"
                OPEN_RO -> "r"
            }

            fun mapMode() = when (this) {
                CREATE, OPEN_RW -> FileChannel.MapMode.READ_WRITE
                OPEN_RO -> FileChannel.MapMode.READ_ONLY
            }
        }

        fun create(path: Path): SimpleHashMapEnv<K, V> {
            val verPath = path.resolve(VERSION_FILENAME)
            val verLock = aquireLock(verPath)
            val verBuffer = getVersionBuffer(verPath, Mode.CREATE)
            verBuffer.putLong(0, 0L)
            return SimpleHashMapEnv(path, verBuffer, verLock)
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv<K, V> {
            val verPath = path.resolve(VERSION_FILENAME)
            val verBuffer = getVersionBuffer(verPath, Mode.OPEN_RO)
            return SimpleHashMapROEnv(path, verBuffer)
        }

        fun openWritable(path: Path): SimpleHashMapEnv<K, V> {
            val verPath = path.resolve(VERSION_FILENAME)
            val verLock = aquireLock(verPath)
            val verBuffer = getVersionBuffer(verPath, Mode.OPEN_RW)
            return SimpleHashMapEnv(path, verBuffer, verLock)
        }

        private fun aquireLock(path: Path): FileLock {
            val lockChannel = RandomAccessFile(path.toString(), "rw").channel
            return try {
                lockChannel.tryLock()
                        ?: throw WriteLockException("Cannot aquire a write lock of the file: $path")
            } catch (e: OverlappingFileLockException) {
                throw WriteLockException("Cannot aquire a write lock of the file: $path", e)
            }
        }

        private fun getVersionBuffer(path: Path, mode: Mode): ByteBuffer {
            return RandomAccessFile(path.toString(), mode.mode())
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

    fun getMap(): SimpleHashMap<K, V> {
        val mapPath = getMapPath(getVersion())
        val mapBuffer = RandomAccessFile(mapPath.toString(), "rw").use { file ->
            val channel = file.channel
            channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size())
        }
        return SimpleHashMapImpl(mapBuffer)
    }

    override fun close() {
        versionFileLock.release()
    }
}
