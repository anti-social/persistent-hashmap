package company.evo.persistent.hashmap.robinhood;

import company.evo.io.IOBuffer;
import company.evo.io.MutableIOBuffer;
import company.evo.io.MutableUnsafeBuffer;
import company.evo.io.UnsafeBuffer;
import company.evo.persistent.MappedFile;
import company.evo.persistent.hashmap.Dummy32;
import company.evo.rc.AtomicRefCounted;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.FFFF_Result;

import java.nio.ByteBuffer;

import kotlin.Unit;

@JCStressTest
@Outcome(id = "2.0, 2.0, 2.0, 2.0", expect = Expect.ACCEPTABLE, desc = "Ok")
@State
public class RobinHoodHashMapStressTest {
    private final RobinHoodHashMap_Int_Float map;
    private final RobinHoodHashMapRO_Int_Float mapRO;

    public RobinHoodHashMapStressTest() {
        MapInfo mapInfo = MapInfo.Companion.calcFor(
            5, 0.75,
            RobinHoodHashMapType_Int_Float.INSTANCE.getBucketLayout().getSize()
        );
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(mapInfo.getBufferSize());
        MutableIOBuffer buffer = new MutableUnsafeBuffer(byteBuffer);
        mapInfo.initBuffer(
            buffer,
            RobinHoodHashMapType_Int_Float.INSTANCE.getKeySerializer(),
            RobinHoodHashMapType_Int_Float.INSTANCE.getValueSerializer(),
            RobinHoodHashMapType_Int_Float.INSTANCE.getHasherProvider()
                .getHasher(Dummy32.INSTANCE.getSerial())
        );

        map = new RobinHoodHashMap_Int_Float(
            0L,
            new AtomicRefCounted<>(
                new MappedFile<>("<map>", buffer),
                (buf) -> Unit.INSTANCE
            )
        );
        assert map.getCapacity() == 7;
        map.put(1, 1);
        map.put(2, 2);

        IOBuffer roBuffer = new UnsafeBuffer(byteBuffer);
        mapRO = new RobinHoodHashMapRO_Int_Float(
            0L,
            new AtomicRefCounted<>(
                new MappedFile<>("<ro-map>", roBuffer),
                (buf) -> Unit.INSTANCE
            )
        );
    }

    @Actor
    public void writer() {
        map.put(8, 108);
        map.remove(8);
        map.put(15, 15);
        map.remove(15);
    }

    @Actor
    public void reader1(FFFF_Result r) {
        r.r1 = mapRO.get(2, -1);
        r.r2 = mapRO.get(2, -1);
        r.r3 = mapRO.get(2, -1);
        r.r4 = mapRO.get(2, -1);
    }
}
