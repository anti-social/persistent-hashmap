package company.evo.persistent.hashmap.straight

import company.evo.io.MutableUnsafeBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.BaseState
import company.evo.persistent.hashmap.Dummy32
import company.evo.persistent.hashmap.Hash32
import company.evo.persistent.hashmap.Knuth32
import company.evo.rc.AtomicRefCounted

import org.openjdk.jmh.annotations.Benchmark
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
    open class StraightHashMapState : BaseState() {
        var map: StraightHashMap_Int_Float? = null

        @Param(
            "hash32",
            "knuth32",
            "dummy32"
        )
        private var hashAlgo: String = ""

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

    @Benchmark
    @Threads(1)
    open fun benchmark_1_reader(state: StraightHashMapState, blackhole: Blackhole) {
        val map = state.map!!
        for (k in state.dataSet.lookupKeys) {
            blackhole.consume(
                map.get(k, 0.0F)
            )
        }
    }
}