package company.evo.persistent

import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

class VersionedRamDirectory private constructor(
        private val bufferAllocator: (Int) -> ByteBuffer
) : AbstractVersionedDirectory(
        bufferAllocator(VersionedDirectory.VERSION_LENGTH)
                .order(VersionedDirectory.BYTE_ORDER)
) {

    private val buffers = ConcurrentHashMap<String, ByteBuffer>()

    companion object {
        private val HEAP_ALLOCATOR = ByteBuffer::allocate
        private val DIRECT_ALLOCATOR = ByteBuffer::allocateDirect

        fun createHeap(): VersionedRamDirectory {
            return VersionedRamDirectory(HEAP_ALLOCATOR)
        }

        fun createDirect(): VersionedRamDirectory {
            return VersionedRamDirectory(DIRECT_ALLOCATOR)
        }
    }

    override fun createFile(name: String, size: Int): ByteBuffer {
        val buffer = bufferAllocator(size)
                .order(VersionedDirectory.BYTE_ORDER)
        buffers[name] = buffer
        return buffer
    }

    override fun openFileWritable(name: String): ByteBuffer {
        return (buffers[name] ?: throw FileDoesNotExistException(Paths.get(name)))
                .duplicate()
                .clear()
                .order(VersionedDirectory.BYTE_ORDER)
    }

    override fun openFileReadOnly(name: String): ByteBuffer {
        return (buffers[name] ?: throw FileDoesNotExistException(Paths.get(name)))
                .duplicate()
                .clear()
                .order(VersionedDirectory.BYTE_ORDER)
                .asReadOnlyBuffer()
    }

    override fun deleteFile(name: String) {
        buffers.remove(name)
    }

    override fun close() {
        super.close()
        buffers.clear()
    }
}
