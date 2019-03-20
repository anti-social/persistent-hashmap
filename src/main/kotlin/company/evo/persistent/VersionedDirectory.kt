package company.evo.persistent

import java.io.Closeable
import java.nio.ByteOrder
import java.nio.file.Path

import org.agrona.concurrent.AtomicBuffer

open class VersionedDirectoryException(
        msg: String, cause: Exception? = null
) : Exception(msg, cause)
class WriteLockException(
        msg: String, cause: Exception? = null
) : VersionedDirectoryException(msg, cause)
class CorruptedVersionFileException(msg: String) : VersionedDirectoryException(msg)
class ReadOnlyException(msg: String) : VersionedDirectoryException(msg)
class FileAlreadyExistsException(path: Path) : VersionedDirectoryException("Cannot create $path: already exists")
class FileDoesNotExistException(path: Path) : VersionedDirectoryException("Cannot open $path: does not exist")

interface VersionedDirectory : Closeable {
    fun readVersion(): Long
    fun writeVersion(version: Long)
    fun createFile(name: String, size: Int): AtomicBuffer
    fun openFileWritable(name: String): AtomicBuffer
    fun openFileReadOnly(name: String): AtomicBuffer
    fun deleteFile(name: String)

    companion object {
        const val VERSION_LENGTH = 8
        val BYTE_ORDER: ByteOrder = ByteOrder.LITTLE_ENDIAN
    }
}

abstract class AbstractVersionedDirectory(
        private val versionBuffer: AtomicBuffer
) : VersionedDirectory {

    override fun readVersion() = versionBuffer.getLongVolatile(0)

    override fun writeVersion(version: Long) {
        versionBuffer.putLongVolatile(0, version)
    }

    override fun close() {}
}
