package dev.evo.io

import java.nio.Buffer
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.ByteOrder

import sun.misc.Unsafe

import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.lang.foreign.MemoryLayout
import java.lang.foreign.ValueLayout
import java.lang.invoke.MethodHandles

open class MemorySegmentBuffer(protected val segment: MemorySegment, protected val arena: Arena) : IOBuffer {
    protected val byteHandle = ValueLayout.JAVA_BYTE.varHandle()
        .let { h -> MethodHandles.insertCoordinates(h, 0, segment) }
    protected val shortHandle = ValueLayout.JAVA_SHORT.varHandle()
        .let { h -> MethodHandles.insertCoordinates(h, 0, segment) }
    protected val intHandle = ValueLayout.JAVA_INT.varHandle()
        .let { h -> MethodHandles.insertCoordinates(h, 0, segment) }
    protected val longHandle = ValueLayout.JAVA_LONG.varHandle()
        .let { h -> MethodHandles.insertCoordinates(h, 0, segment) }
    protected val floatHandle = ValueLayout.JAVA_FLOAT.varHandle()
        .let { h -> MethodHandles.insertCoordinates(h, 0, segment) }
    protected val doubleHandle = ValueLayout.JAVA_DOUBLE.varHandle()
        .let { h -> MethodHandles.insertCoordinates(h, 0, segment) }

    override fun getByteBuffer(): ByteBuffer? {
        return segment.asByteBuffer()
    }

    override fun isDirect(): Boolean {
        return segment.isNative() || segment.isMapped()
    }

    override fun size(): Int {
        return segment.byteSize().toInt()
    }

    override fun drop() {
        arena.close()
    }

    override fun readBytes(ix: Int, dst: ByteArray) {
        segment.asByteBuffer().get(ix, dst)
    }

    override fun readBytes(ix: Int, dst: ByteArray, offset: Int, length: Int) {
        segment.asByteBuffer().get(ix, dst, offset, length)
    }

    override fun readByte(ix: Int): Byte {
        return segment.get(ValueLayout.JAVA_BYTE, ix.toLong())
    }

    override fun readShort(ix: Int): Short {
        return segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, ix.toLong())
    }

    override fun readInt(ix: Int): Int {
        return segment.get(ValueLayout.JAVA_INT_UNALIGNED, ix.toLong())
    }

    override fun readLong(ix: Int): Long {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, ix.toLong())
    }

    override fun readFloat(ix: Int): Float {
        return segment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, ix.toLong())
    }

    override fun readDouble(ix: Int): Double {
        return segment.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, ix.toLong())
    }

    override fun readByteVolatile(ix: Int): Byte {
        return byteHandle.getVolatile(segment, ix.toLong()) as Byte
    }

    override fun readShortVolatile(ix: Int): Short {
        return shortHandle.getVolatile(ix.toLong()) as Short
    }

    override fun readIntVolatile(ix: Int): Int {
        return intHandle.getVolatile(ix.toLong()) as Int
    }

    override fun readLongVolatile(ix: Int): Long {
        return longHandle.getVolatile(ix.toLong()) as Long
    }

    override fun readFloatVolatile(ix: Int): Float {
        return floatHandle.getVolatile(ix.toLong()) as Float
    }

    override fun readDoubleVolatile(ix: Int): Double {
        return doubleHandle.getVolatile(ix.toLong()) as Double
    }
}

class MutableMemorySegmentBuffer(
    segment: MemorySegment, arena: Arena
) : MemorySegmentBuffer(segment, arena), MutableIOBuffer {
    override fun writeBytes(ix: Int, src: ByteArray) {
        segment.asByteBuffer().put(ix, src)
    }

    override fun writeBytes(ix: Int, src: ByteArray, offset: Int, length: Int) {
        segment.asByteBuffer().put(ix, src, offset, length)
    }

    override fun writeByte(ix: Int, v: Byte) {
        return segment.set(ValueLayout.JAVA_BYTE, ix.toLong(), v)
    }

    override fun writeShort(ix: Int, v: Short) {
        return segment.set(ValueLayout.JAVA_SHORT_UNALIGNED, ix.toLong(), v)
    }

    override fun writeInt(ix: Int, v: Int) {
        return segment.set(ValueLayout.JAVA_INT_UNALIGNED, ix.toLong(), v)
    }

    override fun writeLong(ix: Int, v: Long) {
        return segment.set(ValueLayout.JAVA_LONG_UNALIGNED, ix.toLong(), v)
    }

    override fun writeFloat(ix: Int, v: Float) {
        return segment.set(ValueLayout.JAVA_FLOAT_UNALIGNED, ix.toLong(), v)
    }

    override fun writeDouble(ix: Int, v: Double) {
        return segment.set(ValueLayout.JAVA_DOUBLE_UNALIGNED, ix.toLong(), v)
    }

    override fun writeByteVolatile(ix: Int, v: Byte) {
        byteHandle.setVolatile(ix.toLong(), v)
    }

    override fun writeShortVolatile(ix: Int, v: Short) {
        shortHandle.setVolatile(ix.toLong(), v)
    }

    override fun writeIntVolatile(ix: Int, v: Int) {
        intHandle.setVolatile(ix.toLong(), v)
    }

    override fun writeLongVolatile(ix: Int, v: Long) {
        longHandle.setVolatile(ix.toLong(), v)
    }

    override fun writeFloatVolatile(ix: Int, v: Float) {
        floatHandle.setVolatile(ix.toLong(), v)
    }

    override fun writeDoubleVolatile(ix: Int, v: Double) {
        doubleHandle.setVolatile(ix.toLong(), v)
    }

    override fun writeIntOrdered(ix: Int, v: Int) {
        intHandle.setOpaque(ix.toLong(), v)
    }

    override fun writeLongOrdered(ix: Int, v: Long) {
        longHandle.setOpaque(ix.toLong(), v)
    }

    override fun fsync() {
        segment.force()
    }
}
