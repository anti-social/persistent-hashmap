package company.evo.persistent.hashmap.simple

import java.nio.ByteBuffer

import org.agrona.concurrent.UnsafeBuffer

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

open class SimpleHashMapBenchmark {
    @State(Scope.Benchmark)
    open class SimpleHashMapState : BaseState() {
        var map: SimpleHashMapImpl_K_V? = null
        var i = 0

        @Setup(Level.Trial)
        fun initMap() {
            val bucketLayout = SimpleHashMap_K_V.bucketLayout_K_V()
            val mapInfo = MapInfo.calcFor(entries, 0.5, bucketLayout.size)
            val buffer = ByteBuffer.allocateDirect(mapInfo.bufferSize)
            SimpleHashMap_K_V.initBuffer(UnsafeBuffer(buffer), bucketLayout, mapInfo)
            map = SimpleHashMapImpl_K_V(0L, UnsafeBuffer(buffer), bucketLayout, DefaultStatsCollector())

            val keys = intKeys.asSequence().take(entries)
            val values = doubleValues.asSequence().map { it.toFloat() }.take(entries)
            keys.zip(values).forEach { (k, v) ->
                map!!.put(k, v)
            }
        }

        @TearDown
        fun printStats() {
            println()
            println("======== Map Info ========")
            println(map?.toString())
            println("===========================")
            println("======== Map Stats ========")
            println(map?.stats())
            println("===========================")
        }
    }

    @Benchmark
    @Threads(1)
    open fun benchmark_1_reader(state: SimpleHashMapState, blackhole: Blackhole) {
        // blackhole.consume(
        //         state.map.get(BaseState.intKeys[state.i], 0.0F)
        // )
        // state.i++
        // state.i = state.i and BaseState.IX_MASK
        val map = state.map
        for (ix in BaseState.ixs) {
            blackhole.consume(
                    map!!.get(BaseState.intKeys[ix], 0.0F)
            )
        }
    }
}
