package company.evo.persistent.hashmap

import sun.misc.Unsafe
import java.nio.ByteBuffer

abstract class BucketSerializer(
        val bucketLayout: BucketLayout,
        protected val buffer: ByteBuffer
) {
    open var offset: Int = 0

    private fun readRaw(fieldOffset: Int, fieldSize: Int, array: ByteArray) {
        buffer.position(offset + fieldOffset)
        buffer.get(array, 0, fieldSize)
    }
    fun readRawKey(array: ByteArray) =
            readRaw(bucketLayout.keyOffset, bucketLayout.keySize, array)
    fun readRawValue(array: ByteArray) =
            readRaw(bucketLayout.valueOffset, bucketLayout.valueSize, array)
}

class ShortIntFloatBucketSerializer(buffer: ByteBuffer) :
        BucketSerializer(BucketLayout(2, 4, 4), buffer)
{
    fun readMeta() = buffer.getShort(offset + bucketLayout.metaOffset)
    fun writeMeta(meta: Short) {
        buffer.putShort(offset + bucketLayout.metaOffset, meta)
    }
    fun readKey() = buffer.getInt(offset + bucketLayout.keyOffset)
    fun writeKey(key: Int) {
        buffer.putInt(offset + bucketLayout.keyOffset, key)
    }
    fun readValue() = buffer.getFloat(offset + bucketLayout.valueOffset)
    fun writeValue(value: Float) {
        buffer.putFloat(offset + bucketLayout.valueOffset, value)
    }
}

abstract class UnsafeBucketSerializer(
        bucketLayout: BucketLayout,
        buffer: ByteBuffer
) : BucketSerializer(bucketLayout, buffer) {
    companion object {
        private val UNSAFE: Unsafe

        init {
            val field = Unsafe::class.java.getDeclaredField("theUnsafe")
            field.setAccessible(true)
            UNSAFE = field.get(null) as Unsafe
        }
    }

    protected val unsafe
        get() = UNSAFE

    private val baseAddress: Long

    init {
        val addressField = java.nio.Buffer::class.java.getDeclaredField("address")
        addressField.setAccessible(true)
        baseAddress = addressField.getLong(buffer)
    }

    protected var address: Long = baseAddress
        private set
    override var offset: Int = 0
        set(bucketOffset) {
            if (bucketOffset < 0 || bucketOffset + bucketLayout.size >= buffer.capacity()) {
                throw IndexOutOfBoundsException()
            }
            field = bucketOffset
            address = baseAddress + bucketOffset
        }
}

class Unsafe_ShortIntFloatBucketSerializer(buffer: ByteBuffer) :
        UnsafeBucketSerializer(BucketLayout(2, 4, 4), buffer)
{
    fun readMeta() = unsafe.getShort(address + bucketLayout.metaOffset)
    fun writeMeta(meta: Short) {
        unsafe.putShort(address + bucketLayout.metaOffset, meta)
    }
    fun readKey() = unsafe.getInt(address + bucketLayout.keyOffset)
    fun writeKey(key: Int) {
        unsafe.putInt(address + bucketLayout.keyOffset, key)
    }
    fun readValue() = unsafe.getFloat(address + bucketLayout.valueOffset)
    fun writeValue(value: Float) {
        unsafe.putFloat(address + bucketLayout.valueOffset, value)
    }
}