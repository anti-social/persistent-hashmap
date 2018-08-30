package company.evo.persistent.hashmap.simple;

import company.evo.persistent.hashmap.BucketLayout;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.I_Result;
import org.openjdk.jcstress.infra.results.IIIIII_Result;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@JCStressTest
@Outcome(id = "108", expect = Expect.ACCEPTABLE, desc = "Ok")
// @Outcome(id = "101, -1", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Ok - one value is missing")
// @Outcome(id = "-1, 101", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Ok - one value is missing")
// @Outcome(id = "-1, -1", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Ok - both values are missing")
@State
public class SimpleHashMapStressTest {
    private final SimpleHashMap<Integer, Integer> map;
    private final SimpleHashMapRO<Integer, Integer> mapRO;

    public SimpleHashMapStressTest() {
        BucketLayout<Integer, Integer> bucketLayout =
                SimpleHashMap.Companion.bucketLayout(Integer.class, Integer.class);
        MapInfo mapInfo = MapInfo.Companion.calcFor(5, 0.75, bucketLayout.getSize());
        ByteBuffer buffer = ByteBuffer.allocate(mapInfo.getBufferSize()).order(ByteOrder.nativeOrder());
        SimpleHashMap.Companion.initBuffer(buffer, bucketLayout, mapInfo);
        map = new SimpleHashMapImpl<>(0L, buffer, bucketLayout);
        assert map.getCapacity() == 7;
        map.put(1, 101);
        map.put(8, 108);
        ByteBuffer roBuffer = buffer.duplicate().clear().order(ByteOrder.nativeOrder());
        System.out.println(roBuffer);
        mapRO = new SimpleHashMapROImpl<>(0L, roBuffer, bucketLayout);
    }

    @Actor
    public void writer() {
        map.remove(8);
        map.put(15, 115);
        map.remove(15);
        map.put(8, 108);
    }

    @Actor
    public void reader1(I_Result r) {
        r.r1 = mapRO.get(8, 108);
    }

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