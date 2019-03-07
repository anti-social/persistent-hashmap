package company.evo.persistent

import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path

import org.agrona.concurrent.AtomicBuffer
import org.agrona.concurrent.UnsafeBuffer

class VersionedMmapDirectory private constructor(
        val path: Path,
        val versionFilename: String,
        versionBuffer: AtomicBuffer,
        private val writeLock: FileLock? = null,
        val created: Boolean = false
) : AbstractVersionedDirectory(versionBuffer) {
    companion object {
        private fun getVersionPath(path: Path, versionFilename: String): Path {
            return path.resolve(versionFilename)
        }

        private fun getVersionBuffer(versionPath: Path, mode: Mode): AtomicBuffer {
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

        fun openWritable(path: Path, versionFilename: String): VersionedMmapDirectory {
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

        fun openReadOnly(path: Path, versionFilename: String): VersionedMmapDirectory {
            val versionPath = getVersionPath(path, versionFilename)
            if (!versionPath.toFile().exists()) {
                throw FileDoesNotExistException(versionPath)
            }
            val versionBuffer = getVersionBuffer(versionPath, Mode.OpenRO())
            return VersionedMmapDirectory(path, versionFilename, versionBuffer)
        }

        private fun mmapFile(filepath: Path, mode: Mode): AtomicBuffer {
            return RandomAccessFile(filepath.toString(), mode.mode).use { file ->
                if (mode is Mode.Create) {
                    file.setLength(mode.size.toLong())
                }
                val mappedBuffer = file.channel.use { channel ->
                    channel
                            .map(mode.mapMode, 0, channel.size())
                            .order(ByteOrder.nativeOrder())
                }
                UnsafeBuffer(mappedBuffer)
            }
        }
    }

    private sealed class Mode(val mode: String, val mapMode: FileChannel.MapMode) {
        class Create(val size: Int) : Mode("rw", FileChannel.MapMode.READ_WRITE)
        class OpenRO : Mode("r", FileChannel.MapMode.READ_ONLY)
        class OpenRW : Mode("rw", FileChannel.MapMode.READ_WRITE)
    }

    override fun close() {
        super.close()
        writeLock?.close()
    }

    val versionPath = getVersionPath(path, versionFilename)

    override fun createFile(name: String, size: Int): AtomicBuffer {
        ensureWriteLock()
        val filepath = path.resolve(name)
        if (filepath.toFile().exists()) {
            throw FileAlreadyExistsException(filepath)
        }
        return mmapFile(filepath, Mode.Create(size))
    }

    override fun openFileWritable(name: String): AtomicBuffer {
        ensureWriteLock()
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException(filepath)
        }
        return mmapFile(filepath, Mode.OpenRW())
    }

    override fun openFileReadOnly(name: String): AtomicBuffer {
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException(filepath)
        }
        return mmapFile(filepath, Mode.OpenRO())
    }

    override fun deleteFile(name: String) {
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException(filepath)
        }
        filepath.toFile().delete()
    }

    private fun ensureWriteLock() {
        writeLock ?: throw ReadOnlyException(
                "Write operation is not allowed for the directory opened in a readonly mode"
        )
    }
}
