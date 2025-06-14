package dev.evo.persistent

import dev.evo.io.BufferCleaner
import dev.evo.io.IOBuffer
import dev.evo.io.MutableIOBuffer
import dev.evo.io.MutableUnsafeBuffer
import dev.evo.io.MutableMemorySegmentBuffer
import dev.evo.rc.AtomicRefCounted
import dev.evo.rc.RefCounted

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.lang.foreign.Arena

class VersionedMmapDirectory private constructor(
    val path: Path,
    private val versionFile: MappedFile<MutableIOBuffer>,
    private val writeLock: VersionLock?,
    private val bufferManagement: BufferManagement,
    val created: Boolean = false,
) : ManageableVersionedDirectory(versionFile, bufferManagement) {

    private class VersionLock(versionPath: Path) : AutoCloseable {
        private val file = RandomAccessFile(versionPath.toString(), "rw")
        private val lock: FileLock = run {
            val (lock, ex) = try {
                file.channel.tryLock() to null
            } catch (e: OverlappingFileLockException) {
                null to e
            }
            if (lock == null || ex != null) {
                throw WriteLockException(
                    "Cannot retain a write lock of the file: $versionPath", ex
                )
            }
            lock
        }

        override fun close() {
            lock.close()
            file.close()
        }
    }

    companion object {
        private fun getVersionPath(path: Path, versionFilename: String): Path {
            return path.resolve(versionFilename)
        }

        private fun getVersionFile(
            versionPath: Path, mode: Mode, bufferManagement: BufferManagement
        ): MappedFile<MutableIOBuffer> {
            val file = mmapFile(versionPath.toFile(), mode, bufferManagement)
            if (file.buffer.size() != VersionedDirectory.VERSION_LENGTH) {
                throw CorruptedVersionFileException(
                        "Version file must have size ${VersionedDirectory.VERSION_LENGTH}"
                )
            }
            return file
        }

        fun openWritable(
            path: Path, versionFilename: String, bufferManagement: BufferManagement
        ): VersionedMmapDirectory {
            val versionPath = getVersionPath(path, versionFilename)
            val (versionFile, created) = if (!versionPath.toFile().exists()) {
                getVersionFile(versionPath, Mode.Create(VersionedDirectory.VERSION_LENGTH), bufferManagement) to true
            } else {
                getVersionFile(versionPath, Mode.OpenRW, bufferManagement) to false
            }
            val versionLock = VersionLock(versionPath)
            return VersionedMmapDirectory(
                path, versionFile, versionLock, bufferManagement, created = created
            )
        }

        fun openReadOnly(
            path: Path, versionFilename: String, bufferManagement: BufferManagement
        ): VersionedMmapDirectory {
            val versionPath = getVersionPath(path, versionFilename)
            if (!versionPath.toFile().exists()) {
                throw FileDoesNotExistException(versionPath)
            }
            val versionFile = getVersionFile(versionPath, Mode.OpenRO, bufferManagement)
            return VersionedMmapDirectory(path, versionFile, null, bufferManagement)
        }

        private fun mmapFile(file: File, mode: Mode, bufferManagement: BufferManagement): MappedFile<MutableIOBuffer> {
            return RandomAccessFile(file, mode.mode).use { f ->
                if (mode is Mode.Create) {
                    f.setLength(mode.size.toLong())
                }
                val buffer = f.channel.use { channel ->
                    when (bufferManagement) {
                        is BufferManagement.Unsafe -> {
                            val mappedBuffer = channel
                                .map(mode.mapMode, 0, channel.size())
                                .order(ByteOrder.nativeOrder())
                            MutableUnsafeBuffer(mappedBuffer)
                        }
                        BufferManagement.MemorySegments -> {
                            val arena = Arena.ofShared()
                            val mappedSegment = channel.map(mode.mapMode, 0, channel.size(), arena)
                            MutableMemorySegmentBuffer(mappedSegment, arena)
                        }
                    }
                }
                MappedFile(file.path, buffer)
            }
        }
    }

    private sealed class Mode(val mode: String, val mapMode: FileChannel.MapMode) {
        class Create(val size: Int) : Mode("rw", FileChannel.MapMode.READ_WRITE)
        object OpenRO : Mode("r", FileChannel.MapMode.READ_ONLY)
        object OpenRW : Mode("rw", FileChannel.MapMode.READ_WRITE)
    }

    override fun close() {
        super.close()
        bufferCleaner(versionFile)
        writeLock?.close()
    }

    override fun createFile(
            name: String, size: Int, deleteOnExit: Boolean
    ): RefCounted<MappedFile<MutableIOBuffer>> {
        ensureWriteLock()
        val filepath = path.resolve(name)
        if (filepath.toFile().exists()) {
            throw FileAlreadyExistsException(filepath)
        }
        val file = filepath.toFile()
        if (deleteOnExit) {
            file.deleteOnExit()
        }
        return AtomicRefCounted(mmapFile(file, Mode.Create(size), bufferManagement), bufferCleaner)
    }

    override fun openFileWritable(name: String): RefCounted<MappedFile<MutableIOBuffer>> {
        ensureWriteLock()
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException(filepath)
        }
        return AtomicRefCounted(mmapFile(filepath.toFile(), Mode.OpenRW, bufferManagement), bufferCleaner)
    }

    override fun openFileReadOnly(name: String): RefCounted<MappedFile<IOBuffer>> {
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException(filepath)
        }
        return AtomicRefCounted(
            mmapFile(filepath.toFile(), Mode.OpenRO, bufferManagement),
            bufferCleaner
        )
    }

    override fun deleteFile(name: String) {
        val filepath = path.resolve(name)
        if (!filepath.toFile().exists()) {
            throw FileDoesNotExistException(filepath)
        }
        filepath.toFile().delete()
    }

    override fun rename(source: String, dest: String) {
        Files.move(path.resolve(source), path.resolve(dest), StandardCopyOption.ATOMIC_MOVE)
    }

    private fun ensureWriteLock() {
        writeLock ?: throw ReadOnlyException(
            "Write operation is not allowed for the directory opened in a readonly mode"
        )
    }
}
