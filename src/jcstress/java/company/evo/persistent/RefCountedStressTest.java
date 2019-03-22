package company.evo.persistent;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.IB_Result;

import kotlin.Unit;

// @JCStressTest
// @Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "Ok")
// @Outcome(id = "100", expect = Expect.ACCEPTABLE, desc = "Ok")
// @State
public class RefCountedStressTest {
    AtomicRefCounted<Integer> rc = new AtomicRefCounted<Integer>(100, (v) -> Unit.INSTANCE);

    @Actor
    public void actor1() {
        rc.release();
    }

    @Actor
    public void actor2(IB_Result r) {
        try {
            r.r1 = rc.acquire();
        } catch (IllegalStateException e) {
            r.r1 = 0;
        }
        rc.release();
    }
}
