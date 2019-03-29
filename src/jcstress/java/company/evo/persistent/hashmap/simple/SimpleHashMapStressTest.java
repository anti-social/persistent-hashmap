package company.evo.persistent.hashmap.simple;

import company.evo.rc.AtomicRefCounted;
import company.evo.persistent.MappedFile;

import org.agrona.concurrent.UnsafeBuffer;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.FFFF_Result;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import kotlin.Unit;

@JCStressTest
@Outcome(id = "108.0, 108.0, 108.0, 108.0", expect = Expect.ACCEPTABLE, desc = "Ok")
@State
public class SimpleHashMapStressTest {
    private final SimpleHashMap_Int_Float map;
    private final SimpleHashMapRO_Int_Float mapRO;

    public SimpleHashMapStressTest() {
        MapInfo mapInfo = MapInfo.Companion.calcFor(
                5, 0.75,
                SimpleHashMap_Int_Float.Companion.getBucketLayout().getSize()
        );
        ByteBuffer buffer = ByteBuffer.allocateDirect(mapInfo.getBufferSize()).order(ByteOrder.nativeOrder());
        SimpleHashMap_Int_Float.Companion.initBuffer(new UnsafeBuffer(buffer), mapInfo);
        MappedFile file = new MappedFile("<map>", new UnsafeBuffer(buffer), buffer);
        map = new SimpleHashMapImpl_Int_Float(
                0L,
                new AtomicRefCounted<>(file, (f) -> Unit.INSTANCE),
                new DummyStatsCollector()
        );
        assert map.getCapacity() == 7;
        map.put(-6, -106);
        map.put(8, 108);
        ByteBuffer roBuffer = buffer.duplicate().clear().order(ByteOrder.nativeOrder());
        MappedFile roFile = new MappedFile("<map>", new UnsafeBuffer(roBuffer), roBuffer);
        mapRO = new SimpleHashMapROImpl_Int_Float(
                0L,
                new AtomicRefCounted<>(roFile, (f) -> Unit.INSTANCE),
                new DummyStatsCollector()
        );
    }

    @Actor
    public void writer() {
        map.remove(8);
        map.put(11, 111);
        map.remove(11);
        map.put(8, 108);
    }

    @Actor
    public void reader1(FFFF_Result r) {
        r.r1 = mapRO.get(8, 108);
        r.r2 = mapRO.get(8, 108);
        r.r3 = mapRO.get(8, 108);
        r.r4 = mapRO.get(8, 108);
    }
}
