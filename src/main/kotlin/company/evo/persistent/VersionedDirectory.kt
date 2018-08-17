package company.evo.persistent

import company.evo.persistent.hashmap.CorruptedVersionFileException
import company.evo.persistent.hashmap.Mode
import company.evo.persistent.hashmap.WriteLockException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path

class WriteLockException(msg: String, cause: Exception? = null) : Exception(msg, cause)

class VersionedDirectory private constructor(
        val path: Path,
        versionFilename: String,
        versionBuffer: ByteBuffer,
        lock: FileLock?
) : AutoCloseable {
    companion object {
        private const val VERSION_LENGTH = 8L

        private fun getMapFilename(version: Long) = "hashmap_$version.data"

        private fun getMapPath(dir: Path, version: Long): Path = dir.resolve(getMapFilename(version))

        private fun getVersionPath(path: Path, versionFilename: String) = path.resolve(versionFilename)

        private fun getVersionBuffer(versionPath: Path, mode: Mode): ByteBuffer {
            return RandomAccessFile(versionPath.toString(), mode.mode())
                    .use { file ->
                        when (mode) {
                            Mode.CREATE -> file.setLength(VERSION_LENGTH)
                            Mode.OPEN_RO, Mode.OPEN_RW -> {
                                if (file.length() != VERSION_LENGTH) {
                                    throw CorruptedVersionFileException(
                                            "Version file must have size $VERSION_LENGTH"
                                    )
                                }
                            }
                        }
                        file.channel.use { channel ->
                            channel
                                    .map(mode.mapMode(), 0, channel.size())
                                    .order(ByteOrder.nativeOrder())
                        }
                    }
        }

        private fun acquireLock(versionPath: Path): FileLock {
            val lockChannel = RandomAccessFile(versionPath.toString(), "rw").channel
            return try {
                lockChannel.tryLock()
                        ?: throw WriteLockException("Cannot acquire a write lock of the file: $versionPath")
            } catch (e: OverlappingFileLockException) {
                throw WriteLockException("Cannot acquire a write lock of the file: $versionPath", e)
            }
        }

        fun openWritable(path: Path, versionFilename: String): VersionedDirectory {
            val versionPath = getVersionPath(path, versionFilename)
            val versionBuffer = if (!versionPath.toFile().exists()) {
                getVersionBuffer(versionPath, Mode.CREATE)
            } else {
                getVersionBuffer(versionPath, Mode.OPEN_RW)
            }
            val versionLock = acquireLock(versionPath)
            return VersionedDirectory(path, versionFilename, versionBuffer, versionLock)
        }

    }

    override fun close() {

    }

    val versionPath = getVersionPath(path, versionFilename)

    fun readVersion() = versionBuffer.getLong(0)

    fun writeVersion(version: Long) = versionBuffer.putLong(0, version)

    fun openMap(version: Long, bufferSize: Int = 0): ByteBuffer {
        return RandomAccessFile(getMapPath(path, version).toString(), mode.mode()).use { file ->
            if (bufferSize > 0) {
                file.setLength(bufferSize.toLong())
            }
            file.channel.use { channel ->
                println("Channel size: ${channel.size()}")
                channel
                        .map(mode.mapMode(), 0, channel.size())
                        .order(ByteOrder.nativeOrder())
            }
        }
    }
}