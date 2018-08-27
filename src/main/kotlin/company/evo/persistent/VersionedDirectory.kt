package company.evo.persistent

import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Path

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
    fun createFile(name: String, size: Int): ByteBuffer
    fun openFileWritable(name: String): ByteBuffer
    fun openFileReadOnly(name: String): ByteBuffer
    fun deleteFile(name: String)

    companion object {
        const val VERSION_LENGTH = 8
        val BYTE_ORDER: ByteOrder = ByteOrder.LITTLE_ENDIAN
    }
}

abstract class AbstractVersionedDirectory(
        private val versionBuffer: ByteBuffer
) : VersionedDirectory {

    override fun readVersion() = versionBuffer.getLong(0)

    override fun writeVersion(version: Long) {
        versionBuffer.putLong(0, version)
    }

    override fun close() {}
}
