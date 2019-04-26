package company.evo.persistent.hashmap

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer

interface Serializer<T> {
    val serial: Long
    val size: Int
    fun read(buf: IOBuffer, offset: Int): T
    fun write(buf: MutableIOBuffer, offset: Int, v: T)

    companion object {
        @Suppress("UNCHECKED_CAST")
        fun <T> getForClass(clazz: Class<T>): Serializer<T> = when (clazz) {
            Int::class.javaPrimitiveType -> Serializer_Int()
            Long::class.javaPrimitiveType -> Serializer_Long()
            Float::class.javaPrimitiveType -> Serializer_Float()
            Double::class.javaPrimitiveType -> Serializer_Double()
            else -> throw IllegalArgumentException("Unsupported class: $clazz")
        } as Serializer<T>
    }
}

class Serializer_Short : Serializer<Short> {
    override val serial = 1L
    override val size = 2
    override fun read(buf: IOBuffer, offset: Int) = buf.readShort(offset)
    override fun write(buf: MutableIOBuffer, offset: Int, v: Short) {
        buf.writeShort(offset, v)
    }
}

class Serializer_Int : Serializer<Int> {
    override val serial = 2L
    override val size = 4
    override fun read(buf: IOBuffer, offset: Int) = buf.readInt(offset)
    override fun write(buf: MutableIOBuffer, offset: Int, v: Int) {
        buf.writeInt(offset, v)
    }
}

class Serializer_Long : Serializer<Long> {
    override val serial = 3L
    override val size = 8
    override fun read(buf: IOBuffer, offset: Int) = buf.readLong(offset)
    override fun write(buf: MutableIOBuffer, offset: Int, v: Long) {
        buf.writeLong(offset, v)
    }
}

class Serializer_Float : Serializer<Float> {
    override val serial = 4L
    override val size = 4
    override fun read(buf: IOBuffer, offset: Int) = buf.readFloat(offset)
    override fun write(buf: MutableIOBuffer, offset: Int, v: Float) {
        buf.writeFloat(offset, v)
    }
}

class Serializer_Double : Serializer<Double> {
    override val serial = 5L
    override val size = 8
    override fun read(buf: IOBuffer, offset: Int) = buf.readDouble(offset)
    override fun write(buf: MutableIOBuffer, offset: Int, v: Double) {
        buf.writeDouble(offset, v)
    }
}

