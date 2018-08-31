package company.evo.persistent.hashmap.simple

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.nio.ByteBuffer

open class SimpleHashMapBenchmark {
    @State(Scope.Benchmark)
    open class SimpleHashMapState : BaseState() {
        lateinit var map: SimpleHashMapImpl<Int, Float>

        @Setup(Level.Trial)
        fun initMap() {
            val bucketLayout = SimpleHashMap.bucketLayout<Int, Float>()
            val mapInfo = MapInfo.calcFor(entries, 0.5, bucketLayout.size)
            val buffer = ByteBuffer.allocateDirect(mapInfo.bufferSize)
            SimpleHashMap.initBuffer(buffer, bucketLayout, mapInfo)
            map = SimpleHashMapImpl(0L, buffer, bucketLayout, StatsCollectorImpl())

            val keys = intKeys.asSequence().take(entries)
            val values = doubleValues.asSequence().map { it.toFloat() }.take(entries)
            keys.zip(values).forEach { (k, v) ->
                map.put(k, v)
            }
        }

        @TearDown
        fun printStats() {
            println()
            println("======== Map Stats ========")
            println(map.stats())
            println("===========================")
        }
    }

    @Benchmark
    @Threads(1)
    open fun benchmark_1_reader(state: SimpleHashMapState, blackhole: Blackhole) {
        for (ix in BaseState.ixs) {
            blackhole.consume(
                    state.map.get(BaseState.intKeys[ix], 0.0F)
            )
        }
    }

}
