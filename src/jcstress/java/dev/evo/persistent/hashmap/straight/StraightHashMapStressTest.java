package dev.evo.persistent.hashmap.straight;

import dev.evo.io.IOBuffer;
import dev.evo.io.MutableIOBuffer;
import dev.evo.io.MutableUnsafeBuffer;
import dev.evo.io.UnsafeBuffer;
import dev.evo.persistent.MappedFile;
import dev.evo.persistent.hashmap.Hash32;
import dev.evo.rc.AtomicRefCounted;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.FFFF_Result;

import java.nio.ByteBuffer;

import kotlin.Unit;

@JCStressTest
@Outcome(id = "108.0, 108.0, 108.0, 108.0", expect = Expect.ACCEPTABLE, desc = "Ok")
@State
public class StraightHashMapStressTest {
    private final StraightHashMap_Int_Float map;
    private final StraightHashMapRO_Int_Float mapRO;

    public StraightHashMapStressTest() {
        MapInfo mapInfo = MapInfo.Companion.calcFor(
                5, 0.75,
                StraightHashMapType_Int_Float.INSTANCE.getBucketLayout().getSize(),
                0
        );
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(mapInfo.getBufferSize());
        MutableIOBuffer buffer = new MutableUnsafeBuffer(byteBuffer);
        mapInfo.initBuffer(
                buffer,
                StraightHashMapType_Int_Float.INSTANCE.getKeySerializer(),
                StraightHashMapType_Int_Float.INSTANCE.getValueSerializer(),
                Hash32.INSTANCE
        );
        map = new StraightHashMapImpl_Int_Float(
                0L,
                new AtomicRefCounted<>(
                        new MappedFile<>("<map>", buffer),
                        (buf) -> Unit.INSTANCE
                )
        );
        assert map.getCapacity() == 7;
        map.put(-6, -106);
        map.put(8, 108);

        IOBuffer roBuffer = new UnsafeBuffer(byteBuffer);
        mapRO = new StraightHashMapROImpl_Int_Float(
                0L,
                new AtomicRefCounted<>(
                        new MappedFile<>("<ro-map>", roBuffer),
                        (buf) -> Unit.INSTANCE
                )
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
