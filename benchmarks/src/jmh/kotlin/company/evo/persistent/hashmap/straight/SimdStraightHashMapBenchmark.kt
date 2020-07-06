package company.evo.persistent.hashmap.straight

import company.evo.io.MutableUnsafeBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.Hash32
import company.evo.rc.AtomicRefCounted

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.nio.ByteBuffer

@Fork(
       value = 1,
       jvmArgsAppend = [
           "-XX:+UseSuperWord",
           "-XX:+UnlockDiagnosticVMOptions",
           "-XX:CompileCommand=print,*SimdStraightHashMapBenchmark.benchmark_1_reader"
       ]
)
open class SimdStraightHashMapBenchmark {
    @State(Scope.Benchmark)
    open class StraightHashMapState : BaseState() {
        var map: StraightHashMapImpl_Int_Float? = null
        var hashes: IntArray? = null

        @Setup(Level.Trial)
        fun initMap() {
            val mapInfo = MapInfo.calcFor(
                    entries,
                    0.5,
                    StraightHashMapType_Int_Float.bucketLayout.size
            )
            val buffer = ByteBuffer.allocateDirect(mapInfo.bufferSize)
            mapInfo.initBuffer(
                    MutableUnsafeBuffer(buffer),
                    StraightHashMapType_Int_Float.keySerializer,
                    StraightHashMapType_Int_Float.valueSerializer,
                    StraightHashMapType_Int_Float.hasherProvider.getHasher(Hash32.serial)
            )
            map = StraightHashMapImpl_Int_Float(
                    0L,
                    AtomicRefCounted(MappedFile("<map>", MutableUnsafeBuffer(buffer))) {},
                    DefaultStatsCollector()
            )

            val keys = intKeys.asSequence().take(entries)
            val values = doubleValues.asSequence().map { it.toFloat() }.take(entries)
            keys.zip(values).forEach { (k, v) ->
                map!!.put(k, v)
            }
            hashes = IntArray(ixs.size)
        }

        @TearDown
        fun printStats() {
            println()
            println("======== Map Info =========")
            println(map?.toString())
            println("======== Get Stats ========")
            println(map?.stats())
            println("===========================")
        }
    }

    private fun hash32(v: Int): Int {
        // hash32 function from https://nullprogram.com/blog/2018/07/31/
        var x = v
        x = (x xor (x ushr 16)) * 0x45d9f3b
        x = (x xor (x ushr 16)) * 0x45d9f3b
        x = x xor (x ushr 16)
        return x
    }

    @Benchmark
    @Threads(1)
    open fun benchmark_1_reader(state: StraightHashMapState, blackhole: Blackhole) {
        val map = state.map!!
        val hashes = state.hashes!!
        for (i in BaseState.ixs.indices) {
            val ix = BaseState.ixs[i]
            hashes[i] = hash32(BaseState.intKeys[ix])
        }
        for (i in BaseState.ixs.indices) {
            val ix = BaseState.ixs[i]
            blackhole.consume(
                    map.get(BaseState.intKeys[ix], hashes[i], 0.0F)
            )
        }
    }
}
