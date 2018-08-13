package company.evo.persistent.hashmap

import java.nio.ByteBuffer

import kotlin.math.abs

interface Serializer<T> {
    companion object {
        @Suppress("UNCHECKED_CAST")
        fun <T> getForClass(clazz: Class<T>): Serializer<T> = when (clazz) {
            Double::class.javaObjectType -> DoubleSerializer()
            Float::class.javaObjectType -> FloatSerializer()
            Long::class.javaObjectType -> LongSerializer()
            Int::class.javaObjectType -> IntSerializer()
            Short::class.javaObjectType -> ShortSerializer()
            else -> throw IllegalArgumentException("Unsupported class: $clazz")
        } as Serializer<T>
    }

    val size: Int
    fun hash(v: T): Int
    fun read(buf: java.nio.ByteBuffer, offset: Int): T
    fun write(buf: java.nio.ByteBuffer, offset: Int, v: T)
}

class DoubleSerializer : Serializer<Double> {
    override val size = 8
    override fun hash(v: Double) = abs(v).toInt()
    override fun read(buf: ByteBuffer, offset: Int) = buf.getDouble(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Double) {
        buf.putDouble(offset, v)
    }
}

class FloatSerializer : Serializer<Float> {
    override val size = 4
    override fun hash(v: Float) = abs(v).toInt()
    override fun read(buf: ByteBuffer, offset: Int) = buf.getFloat(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Float) {
        buf.putFloat(offset, v)
    }
}

class LongSerializer : Serializer<Long> {
    override val size = 8
    override fun hash(v: Long) = abs(v).toInt()
    override fun read(buf: ByteBuffer, offset: Int) = buf.getLong(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Long) {
        buf.putLong(offset, v)
    }
}

class IntSerializer : Serializer<Int> {
    override val size = 4
    override fun hash(v: Int) = abs(v)
    override fun read(buf: ByteBuffer, offset: Int) = buf.getInt(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Int) {
        buf.putInt(offset, v)
    }
}

class ShortSerializer : Serializer<Short> {
    override val size = 2
    override fun hash(v: Short) = abs(v.toInt())
    override fun read(buf: ByteBuffer, offset: Int) = buf.getShort(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Short) {
        buf.putShort(offset, v)
    }
}
