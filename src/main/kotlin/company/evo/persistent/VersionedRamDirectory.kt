package company.evo.persistent

import java.nio.ByteBuffer
import java.nio.ByteOrder

class VersionedRamDirectory(
        versionBuffer: ByteBuffer
) : AbstractVersionedDirectory(versionBuffer) {

    val buffers = hashMapOf<String, ByteBuffer>()

    companion object {
        fun createAnonymous(): VersionedDirectory {
            val versionBuffer = ByteBuffer
                    .allocateDirect(VersionedDirectory.VERSION_LENGTH)
                    .order(VersionedDirectory.VERSION_BYTE_ORDER)
            return VersionedRamDirectory(versionBuffer)
        }
    }

    override fun createFile(name: String, size: Int): ByteBuffer {
        val buffer = ByteBuffer.allocateDirect(size)
                .order(ByteOrder.LITTLE_ENDIAN)
        return buffer
    }

    override fun openFileWritable(name: String): ByteBuffer {
        return buffers[name] ?: throw Exception()
    }

    override fun openFileReadOnly(name: String): ByteBuffer {
        return buffers[name] ?: throw Exception()
    }
}