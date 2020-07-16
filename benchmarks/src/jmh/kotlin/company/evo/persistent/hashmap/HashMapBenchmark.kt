package company.evo.persistent.hashmap

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

open class HashMapBenchmark {
    @State(Scope.Benchmark)
    open class HashMapState : BaseState() {
        var map: HashMap<Int, Float> = HashMap()

        @Setup(Level.Trial)
        fun setUpMap() {
            val map = HashMap<Int, Float>(dataSet.keys.size, 0.5F)
            initMap { k, v -> map[k] = v }

            this.map = map
        }
    }

    @Benchmark
    @Threads(1)
    open fun benchmark_1_reader(state: HashMapState, blackhole: Blackhole) {
        for (k in state.dataSet.lookupKeys) {
            blackhole.consume(
                state.map[k]
            )
        }
    }
}
