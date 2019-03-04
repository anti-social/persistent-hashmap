package company.evo.persistent.hashmap.simple;

import company.evo.persistent.hashmap.BucketLayout_K_V;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.F_Result;
import org.openjdk.jcstress.infra.results.IIIIII_Result;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@JCStressTest
@Outcome(id = "108.0", expect = Expect.ACCEPTABLE, desc = "Ok")
// @Outcome(id = "101, -1", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Ok - one value is missing")
// @Outcome(id = "-1, 101", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Ok - one value is missing")
// @Outcome(id = "-1, -1", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Ok - both values are missing")
@State
public class SimpleHashMapStressTest {
    private final SimpleHashMap_K_V map;
    private final SimpleHashMapRO_K_V mapRO;

    public SimpleHashMapStressTest() {
        BucketLayout_K_V bucketLayout =
                SimpleHashMap_K_V.Companion.bucketLayout_K_V();
        MapInfo mapInfo = MapInfo.Companion.calcFor(5, 0.75, bucketLayout.getSize());
        ByteBuffer buffer = ByteBuffer.allocate(mapInfo.getBufferSize()).order(ByteOrder.nativeOrder());
        SimpleHashMap_K_V.Companion.initBuffer(buffer, bucketLayout, mapInfo);
        map = new SimpleHashMapImpl_K_V(
                0L, buffer, bucketLayout,
                new DummyStatsCollector()//, new DefaultTracingCollector()
        );
        assert map.getCapacity() == 7;
        map.put(1, 101.F);
        map.put(8, 108.F);
        ByteBuffer roBuffer = buffer.duplicate().clear().order(ByteOrder.nativeOrder());
        System.out.println(roBuffer);
        mapRO = new SimpleHashMapROImpl_K_V(
                0L, roBuffer, bucketLayout,
                new DummyStatsCollector()//, new DefaultTracingCollector()
        );
    }

    @Actor
    public void writer() {
        map.remove(8);
        map.put(15, 115.F);
        map.remove(15);
        map.put(8, 108.F);
    }

    @Actor
    public void reader1(F_Result r) {
        r.r1 = mapRO.get(8, 108.F);
    }

//    @Arbiter
//    public void arbiter(F_Result r) {
//        if (r.r1 != 108.F) {
//            try {
//                try (PrintWriter writer = new PrintWriter("/tmp/traces.txt", "UTF-8")) {
//                    writer.println(map.tracing().collect());
//                    writer.println(mapRO.tracing().collect());
//                }
//            } catch (IOException ex) {}
//        }
//    }

    // @Actor
    // public void reader2(IIIIII_Result r) {
    //     r.r2 = mapRO.get(8, 108);
    // }
    //
    // @Actor
    // public void reader3(IIIIII_Result r) {
    //     r.r3= mapRO.get(8, 108);
    // }
    //
    // @Actor
    // public void reader4(IIIIII_Result r) {
    //     r.r4 = mapRO.get(8, 108);
    // }
    //
    // @Actor
    // public void reader5(IIIIII_Result r) {
    //     r.r5 = mapRO.get(8, 108);
    // }
    //
    // @Actor
    // public void reader6(IIIIII_Result r) {
    //     r.r6 = mapRO.get(8, 108);
    // }
}
