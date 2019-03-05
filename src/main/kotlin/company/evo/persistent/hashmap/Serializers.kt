package company.evo.persistent.hashmap

import java.nio.ByteBuffer

import org.agrona.concurrent.AtomicBuffer

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

    val serial: Long
    val size: Int
    fun hash(v: T): Int
    fun read(buf: java.nio.ByteBuffer, offset: Int): T
    fun write(buf: java.nio.ByteBuffer, offset: Int, v: T)
}

class ShortSerializer : Serializer<Short> {
    override val serial = 1L
    override val size = 2
    override fun hash(v: Short) = v.toInt()
    override fun read(buf: ByteBuffer, offset: Int) = buf.getShort(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Short) {
        buf.putShort(offset, v)
    }
}

class IntSerializer : Serializer<Int> {
    override val serial = 2L
    override val size = 4
    override fun hash(v: Int) = v
    override fun read(buf: ByteBuffer, offset: Int) = buf.getInt(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Int) {
        buf.putInt(offset, v)
    }
}

class Serializer_K {
    val serial = 2L
    val size = 4
    fun hash(v: Int) = v
    fun read(buf: AtomicBuffer, offset: Int) = buf.getInt(offset)
    fun write(buf: AtomicBuffer, offset: Int, v: Int) {
        buf.putInt(offset, v)
    }
}

class LongSerializer : Serializer<Long> {
    override val serial = 3L
    override val size = 8
    override fun hash(v: Long) = v.toInt()
    override fun read(buf: ByteBuffer, offset: Int) = buf.getLong(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Long) {
        buf.putLong(offset, v)
    }
}

class FloatSerializer : Serializer<Float> {
    override val serial = 4L
    override val size = 4
    override fun hash(v: Float) = java.lang.Float.floatToIntBits(v)
    override fun read(buf: ByteBuffer, offset: Int) = buf.getFloat(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Float) {
        buf.putFloat(offset, v)
    }
}

class Serializer_V {
    val serial = 4L
    val size = 4
    fun hash(v: Float) = java.lang.Float.floatToIntBits(v)
    fun read(buf: AtomicBuffer, offset: Int) = buf.getFloat(offset)
    fun write(buf: AtomicBuffer, offset: Int, v: Float) {
        buf.putFloat(offset, v)
    }
}

class DoubleSerializer : Serializer<Double> {
    override val serial = 5L
    override val size = 8
    override fun hash(v: Double) = java.lang.Double.doubleToLongBits(v).toInt()
    override fun read(buf: ByteBuffer, offset: Int) = buf.getDouble(offset)
    override fun write(buf: ByteBuffer, offset: Int, v: Double) {
        buf.putDouble(offset, v)
    }
}

