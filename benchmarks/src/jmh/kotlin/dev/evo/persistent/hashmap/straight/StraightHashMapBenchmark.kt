package dev.evo.persistent.hashmap.straight

import dev.evo.io.MutableUnsafeBuffer
import dev.evo.persistent.MappedFile
import dev.evo.persistent.hashmap.BaseState
import dev.evo.persistent.hashmap.Dummy32
import dev.evo.persistent.hashmap.Hash32
import dev.evo.persistent.hashmap.Knuth32
import dev.evo.rc.AtomicRefCounted

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Group
import org.openjdk.jmh.annotations.GroupThreads
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

import java.nio.ByteBuffer

open class StraightHashMapBenchmark {
    @State(Scope.Benchmark)
    open class StraightHashMapHasherState : BaseState() {
        var map: StraightHashMap_Int_Float? = null

        @Param(
            "hash32",
            "knuth32",
            "dummy32"
        )
        private var hashAlgo: String = ""

        @Param(
            "RND",
            "SEQ",
            "MISSING_RND",
            "MISSING_SEQ"
        )
        override var lookupMode: LookupMode = LookupMode.RND

        @Setup(Level.Trial)
        fun setUpMap() {
            val mapInfo = MapInfo.calcFor(
                dataSet.keys.size,
                0.5,
                StraightHashMapType_Int_Float.bucketLayout.size
            )
            val buffer = ByteBuffer.allocateDirect(mapInfo.bufferSize)
            val hasher = when (hashAlgo) {
                "hash32" -> Hash32
                "dummy32" -> Dummy32
                "knuth32" -> Knuth32
                else -> throw IllegalArgumentException("Unknown hash algorithm: $hashAlgo")
            }
            mapInfo.initBuffer(
                MutableUnsafeBuffer(buffer),
                StraightHashMapType_Int_Float.keySerializer,
                StraightHashMapType_Int_Float.valueSerializer,
                StraightHashMapType_Int_Float.hasherProvider.getHasher(hasher.serial)
            )
            val map = StraightHashMapImpl_Int_Float(
                0L,
                AtomicRefCounted(MappedFile("<map>", MutableUnsafeBuffer(buffer))) {}
            )
            initMap { k, v -> map.put(k, v) }
            println()
            println("======== Map Info =========")
            println(map.toString())
            println("===========================")

            this.map = map
        }
    }

    @State(Scope.Benchmark)
    open class StraightHashMapState : BaseState() {
        var map: StraightHashMap_Int_Float? = null
        var mapRO: StraightHashMapRO_Int_Float? = null

        override var lookupMode: LookupMode = LookupMode.RND

        @Setup(Level.Trial)
        fun setUpMap() {
            val mapInfo = MapInfo.calcFor(
                dataSet.keys.size,
                0.5,
                StraightHashMapType_Int_Float.bucketLayout.size
            )
            val buffer = ByteBuffer.allocateDirect(mapInfo.bufferSize)
            mapInfo.initBuffer(
                MutableUnsafeBuffer(buffer),
                StraightHashMapType_Int_Float.keySerializer,
                StraightHashMapType_Int_Float.valueSerializer,
                Hash32
            )
            val map = StraightHashMapImpl_Int_Float(
                0L,
                AtomicRefCounted(MappedFile("<map>", MutableUnsafeBuffer(buffer))) {}
            )
            initMap { k, v -> map.put(k, v) }
            println()
            println("======== Map Info =========")
            println(map.toString())
            println("Load factor: ${(map.size() + map.tombstones()) * 100 / map.capacity}%")
            val mapStat = map.stat()
            println("Max distance: ${mapStat.maxDist}")
            println("===========================")

            this.map = map
            this.mapRO = StraightHashMapROImpl_Int_Float(
                0L,
                AtomicRefCounted(MappedFile("<map>", MutableUnsafeBuffer(buffer))) {}
            )
        }
    }

    @Benchmark
    @Threads(1)
    open fun benchmark_hasher(state: StraightHashMapHasherState, blackhole: Blackhole) {
        val map = state.map!!
        for (k in state.dataSet.lookupKeys) {
            blackhole.consume(
                map.get(k, 0.0F)
            )
        }
    }

    @Benchmark
    @Group("ReadWrite")
    @GroupThreads(1)
    open fun benchmark_1_writer(state: StraightHashMapState, blackhole: Blackhole) {
        val map = state.map!!
        for (k in state.dataSet.lookupKeys) {
            blackhole.consume(
                map.put(k, 99.9F)
            )
        }
    }

    @Benchmark
    @Group("ReadWrite")
    @GroupThreads(3)
    open fun benchmark_3_readers(state: StraightHashMapState, blackhole: Blackhole) {
        val map = state.mapRO!!
        for (k in state.dataSet.lookupKeys) {
            blackhole.consume(
                map.get(k, 0.0F)
            )
        }
    }
}
