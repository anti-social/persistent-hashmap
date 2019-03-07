package company.evo.persistent.hashmap

import java.nio.ByteBuffer

import org.agrona.DirectBuffer
import org.agrona.MutableDirectBuffer

interface Serializer<T> {
    val serial: Long
    val size: Int
    fun hash(v: T): Int
    fun read(buf: DirectBuffer, offset: Int): T
    fun write(buf: MutableDirectBuffer, offset: Int, v: T)
}

class Serializer_Int : Serializer<Int> {
    override val serial = 2L
    override val size = 4
    override fun hash(v: Int): Int {
        var x = v
        x = ((x ushr 16) xor x) * 0x45d9f3b
        x = ((x ushr 16) xor x) * 0x45d9f3b
        x = (x ushr 16) xor x
        return x
    }
    override fun read(buf: DirectBuffer, offset: Int) = buf.getInt(offset)
    override fun write(buf: MutableDirectBuffer, offset: Int, v: Int) {
        buf.putInt(offset, v)
    }
}

class Serializer_Long : Serializer<Long> {
    override val serial = 3L
    override val size = 8
    override fun hash(v: Long) = v.toInt()
    override fun read(buf: DirectBuffer, offset: Int) = buf.getLong(offset)
    override fun write(buf: MutableDirectBuffer, offset: Int, v: Long) {
        buf.putLong(offset, v)
    }
}

class Serializer_Float : Serializer<Float> {
    override val serial = 4L
    override val size = 4
    override fun hash(v: Float) = java.lang.Float.floatToIntBits(v)
    override fun read(buf: DirectBuffer, offset: Int) = buf.getFloat(offset)
    override fun write(buf: MutableDirectBuffer, offset: Int, v: Float) {
        buf.putFloat(offset, v)
    }
}

class Serializer_Double : Serializer<Double> {
    override val serial = 5L
    override val size = 8
    override fun hash(v: Double) = java.lang.Double.doubleToLongBits(v).toInt()
    override fun read(buf: DirectBuffer, offset: Int) = buf.getDouble(offset)
    override fun write(buf: MutableDirectBuffer, offset: Int, v: Double) {
        buf.putDouble(offset, v)
    }
}

