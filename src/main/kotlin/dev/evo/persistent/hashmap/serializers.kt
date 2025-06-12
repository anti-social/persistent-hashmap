package dev.evo.persistent.hashmap

import dev.evo.io.IOBuffer
import dev.evo.io.MutableIOBuffer

interface Serializer {
    val serial: Long
    val size: Int

    companion object {
        fun getBySerial(serial: Long): Serializer = when (serial) {
            Serializer_Short.serial -> Serializer_Short
            Serializer_Int.serial -> Serializer_Int
            Serializer_Long.serial -> Serializer_Long
            Serializer_Float.serial -> Serializer_Float
            Serializer_Double.serial -> Serializer_Double
            else -> throw IllegalArgumentException("Unsupported serializer serial: $serial")
        }
    }
}

object Serializer_Short : Serializer {
    override val serial = 1L
    override val size = 2
    fun read(buf: IOBuffer, offset: Int) = buf.readShort(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Short) {
        buf.writeShort(offset, v)
    }
}

object Serializer_Int : Serializer {
    override val serial = 2L
    override val size = 4
    fun read(buf: IOBuffer, offset: Int) = buf.readInt(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Int) {
        buf.writeInt(offset, v)
    }
}

object Serializer_Long : Serializer {
    override val serial = 3L
    override val size = 8
    fun read(buf: IOBuffer, offset: Int) = buf.readLong(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Long) {
        buf.writeLong(offset, v)
    }
}

object Serializer_Float : Serializer {
    override val serial = 4L
    override val size = 4
    fun read(buf: IOBuffer, offset: Int) = buf.readFloat(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Float) {
        buf.writeFloat(offset, v)
    }
}

object Serializer_Double : Serializer {
    override val serial = 5L
    override val size = 8
    fun read(buf: IOBuffer, offset: Int) = buf.readDouble(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Double) {
        buf.writeDouble(offset, v)
    }
}

