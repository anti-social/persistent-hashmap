package company.evo.persistent

import company.evo.rc.AtomicRefCounted
import company.evo.rc.RefCounted
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

import org.agrona.concurrent.UnsafeBuffer

class VersionedRamDirectory private constructor(
        private val bufferAllocator: (Int) -> ByteBuffer,
        private val isDirect: Boolean
) : AbstractVersionedDirectory(
        UnsafeBuffer(
                bufferAllocator(VersionedDirectory.VERSION_LENGTH)
                        .order(VersionedDirectory.BYTE_ORDER)
        )
) {

    private val buffers = ConcurrentHashMap<String, RefCounted<MappedFile>>()

    private var bufferCleaner: (MappedFile) -> Unit = {}
    var useUnmapHack = false
        set(useUnmapHack) {
            if (useUnmapHack)
                throw IllegalArgumentException("useUnmapHack($useUnmapHack)")
            if (!isDirect) {
                throw IllegalArgumentException("Only direct buffers support unmappping")
            }
            if (useUnmapHack && BufferCleaner.BUFFER_CLEANER == null) {
                throw IllegalArgumentException(BufferCleaner.UNMAP_NOT_SUPPORTED_REASON)
            }
            field = useUnmapHack
            bufferCleaner = { file ->
                BufferCleaner.BUFFER_CLEANER?.clean(file.rawBuffer)
            }
        }

    companion object {
        private val HEAP_ALLOCATOR = ByteBuffer::allocate
        private val DIRECT_ALLOCATOR = ByteBuffer::allocateDirect

        fun createHeap(): VersionedRamDirectory {
            return VersionedRamDirectory(HEAP_ALLOCATOR, false)
        }

        fun createDirect(): VersionedRamDirectory {
            return VersionedRamDirectory(DIRECT_ALLOCATOR, true)
        }
    }

    override fun createFile(name: String, size: Int): RefCounted<MappedFile> {
        if (buffers.containsKey(name)) {
            throw FileAlreadyExistsException(Paths.get(name))
        }
        val byteBuffer = bufferAllocator(size)
                .order(VersionedDirectory.BYTE_ORDER)
        val buffer = UnsafeBuffer(byteBuffer)
        val file = AtomicRefCounted(MappedFile(name, buffer, byteBuffer)) {}
        buffers[name] = file
        return file
    }

    override fun openFileWritable(name: String): RefCounted<MappedFile> {
        return buffers.computeIfPresent(name) { _, file ->
            file.also { it.retain() }
        } ?: throw FileDoesNotExistException(Paths.get(name))
    }

    override fun openFileReadOnly(name: String): RefCounted<MappedFile> {
        return openFileWritable(name)
    }

    override fun deleteFile(name: String) {
        (buffers.remove(name) ?: throw FileDoesNotExistException(Paths.get(name)))
                .also { it.release() }
    }

    override fun close() {
        super.close()
        buffers.forEach { (_, file) -> file.release() }
        buffers.clear()
    }
}
