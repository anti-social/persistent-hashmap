package company.evo.persistent.hashmap

import gnu.trove.map.hash.TIntFloatHashMap

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

open class TroveHashMapBenchmark {
    @State(Scope.Benchmark)
    open class TroveHashMapState : BaseState() {
        var map: TIntFloatHashMap = TIntFloatHashMap()

        @Setup(Level.Trial)
        fun setUpMap() {
            val map = TIntFloatHashMap(dataSet.keys.size, 0.5F, 0, 0.0F)
            initMap { k, v -> map.put(k, v) }
            println()
            println("======== Map Info =========")
            println("Capacity: ${map.capacity()}")
            println("===========================")

            this.map = map
        }
    }

    @Benchmark
    @Threads(1)
    open fun benchmark_1_reader(state: TroveHashMapState, blackhole: Blackhole) {
        for (k in state.dataSet.lookupKeys) {
            blackhole.consume(
                state.map.get(k)
            )
        }
    }
}
