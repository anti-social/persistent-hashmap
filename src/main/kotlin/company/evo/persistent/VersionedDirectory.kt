package company.evo.persistent

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path

open class VersionedDirectoryException(
        msg: String, cause: Exception? = null
) : Exception(msg, cause)
class WriteLockException(
        msg: String, cause: Exception? = null
) : VersionedDirectoryException(msg, cause)
class CorruptedVersionFileException(msg: String) : VersionedDirectoryException(msg)
class ReadOnlyException(msg: String) : VersionedDirectoryException(msg)
class FileAlreadyExistsException(msg: String) : VersionedDirectoryException(msg)
class FileDoesNotExistException(msg: String) : VersionedDirectoryException(msg)

interface VersionedDirectory {
    fun readVersion(): Long
    fun writeVersion(version: Long)
    fun createFile(name: String, size: Int): ByteBuffer
    fun openFileWritable(name: String): ByteBuffer
    fun openFileReadOnly(name: String): ByteBuffer

    companion object {
        const val VERSION_LENGTH = 8
        val VERSION_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN
    }
}

abstract class AbstractVersionedDirectory(
        private val versionBuffer: ByteBuffer
) : VersionedDirectory {

    override fun readVersion() = versionBuffer.getLong(0)

    override fun writeVersion(version: Long) {
        versionBuffer.putLong(0, version)
    }
}

class VersionedMmapDirectory private constructor(
        val path: Path,
        val versionFilename: String,
        versionBuffer: ByteBuffer,
        private val writeLock: FileLock? = null,
        val created: Boolean = false
) : AutoCloseable, AbstractVersionedDirectory(versionBuffer) {
    companion object {
        private fun getVersionPath(path: Path, versionFilename: String): Path {
            return path.resolve(versionFilename)
        }

        private fun getVersionBuffer(versionPath: Path, mode: Mode): ByteBuffer {
            val versionBuffer = mmapFile(versionPath, mode)
            if (versionBuffer.capacity() != VersionedDirectory.VERSION_LENGTH) {
                throw CorruptedVersionFileException(
                        "Version file must have size ${VersionedDirectory.VERSION_LENGTH}"
                )
            }
            return versionBuffer
        }

        private fun acquireLock(versionPath: Path): FileLock {
            val lockChannel = RandomAccessFile(versionPath.toString(), "rw").channel
            return try {
                lockChannel.tryLock()
                        ?: throw WriteLockException(
                                "Cannot acquire a write lock of the file: $versionPath"
                        )
            } catch (e: OverlappingFileLockException) {
                throw WriteLockException(
                        "Cannot acquire a write lock of the file: $versionPath", e
                )
            }
        }

        fun openWritable(path: Path, versionFilename: String): VersionedDirectory {
            val versionPath = getVersionPath(path, versionFilename)
            val (versionBuffer, created) = if (!versionPath.toFile().exists()) {
                getVersionBuffer(versionPath, Mode.Create(VersionedDirectory.VERSION_LENGTH)) to true
            } else {
                getVersionBuffer(versionPath, Mode.OpenRW()) to false
            }
            val versionLock = acquireLock(versionPath)
            return VersionedMmapDirectory(
                    path, versionFilename, versionBuffer, versionLock, created = created
            )
        }

        fun openReadOnly(path: Path, versionFilename: String): VersionedDirectory {
            val versionPath = getVersionPath(path, versionFilename)
            if (!versionPath.toFile().exists()) {
                throw FileDoesNotExistException("Version file $versionPath does not exist")
            }
            val versionBuffer = getVersionBuffer(versionPath, Mode.OpenRO())
            return VersionedMmapDirectory(path, versionFilename, versionBuffer)
        }

        private fun mmapFile(filepath: Path, mode: Mode): ByteBuffer {
            return RandomAccessFile(filepath.toString(), mode.mode).use { file ->
                if (mode is Mode.Create) {
                    file.setLength(mode.size.toLong())
                }
                file.channel.use { channel ->
                    channel
                            .map(mode.mapMode, 0, channel.size())
                            .order(ByteOrder.nativeOrder())
                }
            }
        }
    }

    private sealed class Mode(val mode: String, val mapMode: FileChannel.MapMode) {
        class Create(val size: Int) : Mode("rw", FileChannel.MapMode.READ_WRITE)
        class OpenRO : Mode("r", FileChannel.MapMode.READ_ONLY)
        class OpenRW : Mode("rw", FileChannel.MapMode.READ_WRITE)
    }

    override fun close() {
        writeLock?.close()
    }

    val versionPath = getVersionPath(path, versionFilename)

    override fun createFile(name: String, size: Int): ByteBuffer {
        ensureWriteLock()
        val filepath = path.resolve(name)
        if (filepath.toFile().exists()) {
            throw FileAlreadyExistsException("Cannot create $filepath: already exists")
        }
        return mmapFile(filepath, Mode.Create(size))
    }

    override fun openFileWritable(name: String): ByteBuffer {
        ensureWriteLock()
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException("Cannot open $filepath: does not exist")
        }
        return mmapFile(filepath, Mode.OpenRW())
    }

    override fun openFileReadOnly(name: String): ByteBuffer {
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException("Cannot open $filepath: does not exist")
        }
        return mmapFile(filepath, Mode.OpenRO())
    }

    private fun ensureWriteLock() {
        writeLock ?: throw ReadOnlyException(
                "Write operation is not allowed for the directory opened in a readonly mode"
        )
    }
}