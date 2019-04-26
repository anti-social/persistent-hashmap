package company.evo.persistent.hashmap.straight;

import company.evo.io.IOBuffer;
import company.evo.io.MutableIOBuffer;
import company.evo.io.MutableUnsafeBuffer;
import company.evo.io.UnsafeBuffer;
import company.evo.persistent.MappedFile;
import company.evo.rc.AtomicRefCounted;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.FFFF_Result;

import java.nio.ByteBuffer;

import kotlin.Unit;

@JCStressTest
@Outcome(id = "108.0, 108.0, 108.0, 108.0", expect = Expect.ACCEPTABLE, desc = "Ok")
@State
public class SimpleHashMapStressTest {
    private final StraightHashMap_Int_Float map;
    private final StraightHashMapRO_Int_Float mapRO;

    public SimpleHashMapStressTest() {
        MapInfo mapInfo = MapInfo.Companion.calcFor(
                5, 0.75,
                StraightHashMapProvider_Int_Float.INSTANCE.getBucketLayout().getSize()
        );
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(mapInfo.getBufferSize());
        MutableIOBuffer buffer = new MutableUnsafeBuffer(byteBuffer);
        mapInfo.initBuffer(
                buffer,
                StraightHashMapProvider_Int_Float.INSTANCE.getKeySerializer(),
                StraightHashMapProvider_Int_Float.INSTANCE.getValueSerializer()
        );
        map = new StraightHashMapImpl_Int_Float(
                0L,
                new AtomicRefCounted<>(
                        new MappedFile<>("<map>", buffer),
                        (buf) -> Unit.INSTANCE
                ),
                new DummyStatsCollector()
        );
        assert map.getCapacity() == 7;
        map.put(-6, -106);
        map.put(8, 108);

        ByteBuffer roByteBuffer = byteBuffer.duplicate().clear();
        IOBuffer roBuffer = new UnsafeBuffer(roByteBuffer);
        mapRO = new StraightHashMapROImpl_Int_Float(
                0L,
                new AtomicRefCounted<>(
                        new MappedFile<>("<ro-map>", roBuffer),
                        (buf) -> Unit.INSTANCE
                ),
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
