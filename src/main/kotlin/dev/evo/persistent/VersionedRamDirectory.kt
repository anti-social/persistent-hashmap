package dev.evo.persistent

import dev.evo.io.BufferCleaner
import dev.evo.io.IOBuffer
import dev.evo.io.MutableIOBuffer
import dev.evo.io.MutableUnsafeBuffer
import dev.evo.io.MutableMemorySegmentBuffer
import dev.evo.rc.AtomicRefCounted
import dev.evo.rc.RefCounted

import java.lang.foreign.Arena
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

class VersionedRamDirectory private constructor(
    private val bufferManagement: BufferManagement,
    private val bufferAllocator: (Int) -> MutableIOBuffer,
) : ManageableVersionedDirectory(
        MappedFile("<version>", bufferAllocator(VersionedDirectory.VERSION_LENGTH)),
        bufferManagement,
) {
    private val buffers = ConcurrentHashMap<String, RefCounted<MappedFile<MutableIOBuffer>>>()

    companion object {
        fun createHeap(): VersionedRamDirectory {
            return VersionedRamDirectory(BufferManagement.Unsafe(false)) {
                size -> MutableUnsafeBuffer(ByteBuffer.allocate(size))
            }
        }

        fun createDirect(bufferManagement: BufferManagement): VersionedRamDirectory {
            val bufferAllocator: (Int) -> MutableIOBuffer = when (bufferManagement) {
                is BufferManagement.Unsafe -> {
                    { size -> MutableUnsafeBuffer(ByteBuffer.allocateDirect(size)) }
                }
                BufferManagement.MemorySegments -> {
                    { size ->
                        val arena = Arena.ofShared()
                        MutableMemorySegmentBuffer(arena.allocate(size.toLong()), arena)
                    }
                }
            }
            return VersionedRamDirectory(bufferManagement, bufferAllocator) 
            // return VersionedRamDirectory(ByteBuffer::allocateDirect)
        }
    }

    override fun createFile(
        name: String, size: Int, deleteOnExit: Boolean
    ): RefCounted<MappedFile<MutableIOBuffer>> {
        if (buffers.containsKey(name)) {
            throw FileAlreadyExistsException(Paths.get(name))
        }
        val buffer = bufferAllocator(size)
        val file = AtomicRefCounted(MappedFile(name, buffer)) {}
        buffers[name] = file
        return file
    }

    override fun openFileWritable(name: String): RefCounted<MappedFile<MutableIOBuffer>> {
        return buffers.computeIfPresent(name) { _, file ->
            file.also { it.retain() }
        } ?: throw FileDoesNotExistException(Paths.get(name))
    }

    override fun openFileReadOnly(name: String): RefCounted<MappedFile<IOBuffer>> {
        return openFileWritable(name)
    }

    override fun deleteFile(name: String) {
        (buffers.remove(name) ?: throw FileDoesNotExistException(Paths.get(name)))
            .also { it.release() }
    }

    override fun rename(source: String, dest: String) {
        if (buffers.containsKey(dest)) {
            throw FileAlreadyExistsException(Paths.get(dest))
        }
        buffers[dest] = buffers.remove(source) ?: throw FileDoesNotExistException(Paths.get(source))
    }

    override fun close() {
        super.close()
        buffers.forEach { (_, file) -> file.release() }
        buffers.clear()
    }
}
