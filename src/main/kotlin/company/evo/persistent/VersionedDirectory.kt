package company.evo.persistent

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer
import company.evo.rc.RefCounted

import java.io.Closeable
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
     * @return a [MutableIOBuffer] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun createFile(name: String, size: Int): RefCounted<MutableIOBuffer>

    /**
     * Opens an existing file with [name] and maps it into a buffer.
     * @return a [MutableIOBuffer] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun openFileWritable(name: String): RefCounted<MutableIOBuffer>

    /**
     * Opens an existing file with [name] in read-only mode and maps it into a buffer.
     * @return a [MutableIOBuffer] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun openFileReadOnly(name: String): RefCounted<IOBuffer>

    /**
     * Deletes a file with [name].
     */
    fun deleteFile(name: String)

    companion object {
        const val VERSION_LENGTH = 8
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
