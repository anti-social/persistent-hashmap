package dev.evo.persistent

import dev.evo.io.BufferCleaner
import dev.evo.io.IOBuffer
import dev.evo.io.MutableIOBuffer
import dev.evo.rc.RefCounted
import org.slf4j.LoggerFactory

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

data class MappedFile<out T: IOBuffer>(
    val path: String,
    val buffer: T
)

sealed class BufferManagement {
    class Unsafe(val withUnmapHack: Boolean): BufferManagement()
    object MemorySegments: BufferManagement()
}

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
     * If [deleteOnExit] is `true` file will be deleted on virtual machine termination.
     * @return a [MutableIOBuffer] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun createFile(name: String, size: Int, deleteOnExit: Boolean = false): RefCounted<MappedFile<MutableIOBuffer>>

    /**
     * Opens an existing file with [name] and maps it into a buffer.
     * @return a [MutableIOBuffer] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun openFileWritable(name: String): RefCounted<MappedFile<MutableIOBuffer>>

    /**
     * Opens an existing file with [name] in read-only mode and maps it into a buffer.
     * @return a [MutableIOBuffer] wrapped into [RefCounted].
     *
     * Ownership of the returned value is moved to the caller of the method.
     * So it is a user responsibility to close the file.
     */
    fun openFileReadOnly(name: String): RefCounted<MappedFile<IOBuffer>>

    /**
     * Atomically renames a file.
     */
    fun rename(source: String, dest: String)

    /**
     * Deletes a file with [name].
     */
    fun deleteFile(name: String)

    companion object {
        const val VERSION_LENGTH = 8
    }
}

abstract class AbstractVersionedDirectory(
    private val versionFile: MappedFile<MutableIOBuffer>
) : VersionedDirectory {

    override fun readVersion() = versionFile.buffer.readLongVolatile(0)

    override fun writeVersion(version: Long) {
        versionFile.buffer.writeLongVolatile(0, version)
    }

    override fun close() {}
}

abstract class ManageableVersionedDirectory protected constructor(
    private val versionFile: MappedFile<MutableIOBuffer>,
    private val bufferManagement: BufferManagement,
) : AbstractVersionedDirectory(versionFile) {

    private val logger = LoggerFactory.getLogger(ManageableVersionedDirectory::class.java)
    
    protected var bufferCleaner: (MappedFile<IOBuffer>) -> Unit
    init {
        val needCleaner = when (bufferManagement) {
            is BufferManagement.Unsafe -> {
                if (bufferManagement.withUnmapHack) {
                    if (BufferCleaner.BUFFER_CLEANER == null) {
                        throw IllegalArgumentException(BufferCleaner.UNMAP_NOT_SUPPORTED_REASON)
                    }
                    true
                } else {
                    false
                }
            }
            BufferManagement.MemorySegments -> {
                true
            }
        }
        bufferCleaner = if (needCleaner) {
            { file ->
                logger.info("Cleaning file buffer: ${file.path}")
                file.buffer.drop()
            }
        } else {
            {}
        }
    }
}
