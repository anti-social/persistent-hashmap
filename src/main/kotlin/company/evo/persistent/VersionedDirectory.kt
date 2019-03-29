package company.evo.persistent

import company.evo.rc.RefCounted
import java.io.Closeable
import java.nio.ByteBuffer
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

data class MappedFile(
        val buffer: AtomicBuffer,
        val rawBuffer: ByteBuffer
)

interface VersionedDirectory : Closeable {
    /**
     * Reads a version from the version file.
     */
    fun readVersion(): Long

    /**
     * Writes a [version] to the version file.
     */
    fun writeVersion(version: Long)

    /**
     * Creates a file with [name] and [size] and maps it into a buffer.
     * @return a [MappedFile] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun createFile(name: String, size: Int): RefCounted<MappedFile>

    /**
     * Opens an existing file with [name] and maps it into a buffer.
     * @return a [MappedFile] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun openFileWritable(name: String): RefCounted<MappedFile>

    /**
     * Opens an existing file with [name] in read-only mode and maps it into a buffer.
     * @return a [MappedFile] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun openFileReadOnly(name: String): RefCounted<MappedFile>

    /**
     * Deletes a file with [name].
     */
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
