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
    fun put(key: K, value: V)
}

abstract class SimpleHashMapBaseEnv(
        private val versionBuffer: ByteBuffer
) : AutoCloseable
{
    fun getVersion() = versionBuffer.getLong(0)

}

class SimpleHashMapROEnv<K, V>(versionBuffer: ByteBuffer) : SimpleHashMapBaseEnv(versionBuffer) {
    fun aquire(): SimpleHashMapRO<K, V> {
        TODO("not implemented")
    }

    override fun close() {}
}

class SimpleHashMapEnv<K, V>(
        private val versionBuffer: ByteBuffer,
        private val versionFileLock: FileLock
) : SimpleHashMapBaseEnv(versionBuffer) {
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
            return SimpleHashMapEnv(verBuffer, verLock)
        }

        fun openReadOnly(path: Path): SimpleHashMapROEnv<K, V> {
            val verPath = path.resolve(VERSION_FILENAME)
            val verBuffer = getVersionBuffer(verPath, Mode.OPEN_RO)
            return SimpleHashMapROEnv(verBuffer)
        }

        fun openWritable(path: Path): SimpleHashMapEnv<K, V> {
            val verPath = path.resolve(VERSION_FILENAME)
            val verLock = aquireLock(verPath)
            val verBuffer = getVersionBuffer(verPath, Mode.OPEN_RW)
            return SimpleHashMapEnv(verBuffer, verLock)
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

    fun aquire(): SimpleHashMap<K, V> {
        TODO("not implemented")
    }

    override fun close() {
        versionFileLock.release()
    }
}
