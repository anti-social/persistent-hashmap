package company.evo.persistent

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer
import company.evo.rc.RefCounted

import java.io.Closeable
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

// class MappedFile(
//         val buffer:
// )

interface VersionedDirectory : Closeable {
    fun readVersion(): Long
    fun writeVersion(version: Long)
    fun createFile(name: String, size: Int): RefCounted<MutableIOBuffer>
    fun openFileWritable(name: String): RefCounted<MutableIOBuffer>
    fun openFileReadOnly(name: String): RefCounted<IOBuffer>
    fun deleteFile(name: String)

    companion object {
        const val VERSION_LENGTH = 8
        // val BYTE_ORDER: ByteOrder = ByteOrder.LITTLE_ENDIAN
    }
}

abstract class AbstractVersionedDirectory(
        private val versionBuffer: MutableIOBuffer
) : VersionedDirectory {

    override fun readVersion() = versionBuffer.readLongVolatile(0)

    override fun writeVersion(version: Long) {
        versionBuffer.writeLongVolatile(0, version)
    }

    override fun close() {}
}
