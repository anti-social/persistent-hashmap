package company.evo.persistent.hashmap.simple;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

@JCStressTest
@Outcome(id = "1, 2", expect = Expect.ACCEPTABLE, desc = "Ok")
@State
public class SimpleHashMapStressTest {
    @Actor
    public void reader1(II_Result r) {
        r.r1 = 1;
    }

    @Actor
    public void reader2(II_Result r) {
        r.r2 = 2;
    }
}
