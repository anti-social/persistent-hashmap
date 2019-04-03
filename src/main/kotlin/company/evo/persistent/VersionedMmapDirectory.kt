package company.evo.persistent

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer
import company.evo.io.MutableUnsafeBuffer
import company.evo.rc.AtomicRefCounted
import company.evo.rc.RefCounted

import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path


class VersionedMmapDirectory private constructor(
        val path: Path,
        val versionFilename: String,
        private val versionFile: MutableIOBuffer,
        private val writeLock: FileLock? = null,
        val created: Boolean = false
) : AbstractVersionedDirectory(versionFile) {

    val versionPath = getVersionPath(path, versionFilename)

    private var bufferCleaner: (IOBuffer) -> Unit = {}
    var useUnmapHack = false
        set(useUnmapHack) {
            if (useUnmapHack && BufferCleaner.BUFFER_CLEANER == null) {
                throw IllegalArgumentException(BufferCleaner.UNMAP_NOT_SUPPORTED_REASON)
            }
            field = useUnmapHack
            bufferCleaner = { buffer ->
                val byteBuffer = buffer.getByteBuffer()
                if (byteBuffer != null) {
                    BufferCleaner.BUFFER_CLEANER?.clean(byteBuffer)
                }
            }
        }

    companion object {
        private fun getVersionPath(path: Path, versionFilename: String): Path {
            return path.resolve(versionFilename)
        }

        private fun getVersionFile(versionPath: Path, mode: Mode): MutableIOBuffer {
            val buffer = mmapFile(versionPath, mode)
            if (buffer.size() != VersionedDirectory.VERSION_LENGTH) {
                throw CorruptedVersionFileException(
                        "Version file must have size ${VersionedDirectory.VERSION_LENGTH}"
                )
            }
            return buffer
        }

        private fun acquireLock(versionPath: Path): FileLock {
            val lockChannel = RandomAccessFile(versionPath.toString(), "rw").channel
            return try {
                lockChannel.tryLock()
                        ?: throw WriteLockException(
                                "Cannot retain a write lock of the file: $versionPath"
                        )
            } catch (e: OverlappingFileLockException) {
                throw WriteLockException(
                        "Cannot retain a write lock of the file: $versionPath", e
                )
            }
        }

        fun openWritable(path: Path, versionFilename: String): VersionedMmapDirectory {
            val versionPath = getVersionPath(path, versionFilename)
            val (versionFile, created) = if (!versionPath.toFile().exists()) {
                getVersionFile(versionPath, Mode.Create(VersionedDirectory.VERSION_LENGTH)) to true
            } else {
                getVersionFile(versionPath, Mode.OpenRW()) to false
            }
            val versionLock = acquireLock(versionPath)
            return VersionedMmapDirectory(
                    path, versionFilename, versionFile, versionLock, created = created
            )
        }

        fun openReadOnly(path: Path, versionFilename: String): VersionedMmapDirectory {
            val versionPath = getVersionPath(path, versionFilename)
            if (!versionPath.toFile().exists()) {
                throw FileDoesNotExistException(versionPath)
            }
            val versionFile = getVersionFile(versionPath, Mode.OpenRO())
            return VersionedMmapDirectory(path, versionFilename, versionFile)
        }

        private fun mmapFile(filepath: Path, mode: Mode): MutableIOBuffer {
            return RandomAccessFile(filepath.toString(), mode.mode).use { file ->
                if (mode is Mode.Create) {
                    file.setLength(mode.size.toLong())
                }
                val mappedBuffer = file.channel.use { channel ->
                    channel
                            .map(mode.mapMode, 0, channel.size())
                            .order(ByteOrder.nativeOrder())
                }
                MutableUnsafeBuffer(mappedBuffer)
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
        bufferCleaner(versionFile)
        writeLock?.close()
    }

    override fun createFile(name: String, size: Int): RefCounted<MutableIOBuffer> {
        ensureWriteLock()
        val filepath = path.resolve(name)
        if (filepath.toFile().exists()) {
            throw FileAlreadyExistsException(filepath)
        }
        return AtomicRefCounted(mmapFile(filepath, Mode.Create(size)), bufferCleaner)
    }

    override fun openFileWritable(name: String): RefCounted<MutableIOBuffer> {
        ensureWriteLock()
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException(filepath)
        }
        return AtomicRefCounted(mmapFile(filepath, Mode.OpenRW()), bufferCleaner)
    }

    override fun openFileReadOnly(name: String): RefCounted<IOBuffer> {
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException(filepath)
        }
        return AtomicRefCounted(mmapFile(filepath, Mode.OpenRO()), bufferCleaner)
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
