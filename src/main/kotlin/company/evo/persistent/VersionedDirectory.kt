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
    fun readVersion(): Long
    fun writeVersion(version: Long)
    fun createFile(name: String, size: Int): RefCounted<MappedFile>
    fun openFileWritable(name: String): RefCounted<MappedFile>
    fun openFileReadOnly(name: String): RefCounted<MappedFile>
    fun deleteFile(name: String)

    companion object {
        const val VERSION_LENGTH = 8
        val BYTE_ORDER: ByteOrder = ByteOrder.LITTLE_ENDIAN
    }

    // fun openVersionedFile(name: String): RefCounted<VersionedFile> {
    //     var version = readVersion()
    //     while (true) {
    //         val newFile = tryOpenFile(version, name)
    //         if (newFile != null) {
    //             return AtomicRefCounted(newFile, {})
    //         }
    //         val newVersion = readVersion()
    //         if (newVersion == version) {
    //             throw FileDoesNotExistException(Paths.get(getHashmapFilename(version)))
    //         }
    //         version = newVersion
    //     }
    // }
    //
    // private fun tryOpenFile(version: Long, name: String): VersionedFile? {
    //     return try {
    //         VersionedFile(version, openFileReadOnly(name))
    //     } catch (e: FileDoesNotExistException) {
    //         null
    //     }
    // }
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
