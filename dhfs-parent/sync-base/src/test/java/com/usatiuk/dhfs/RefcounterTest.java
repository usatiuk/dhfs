package com.usatiuk.dhfs;

import com.usatiuk.dhfs.refcount.JDataRef;
import com.usatiuk.dhfs.testobjs.TestRefcount;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pcollections.HashTreePSet;

import java.util.List;
import java.util.Map;

class Profiles {
    public static class RefcounterTestProfile extends TempDataProfile {
        @Override
        protected void getConfigOverrides(Map<String, String> ret) {
            ret.put("quarkus.log.category.\"com.usatiuk.dhfs\".level", "INFO");
            ret.put("dhfs.fuse.enabled", "false");
            ret.put("dhfs.objects.ref_verification", "false");
        }
    }
}

@QuarkusTest
@TestProfile(Profiles.RefcounterTestProfile.class)
public class RefcounterTest {

    @Inject
    Transaction curTx;
    @Inject
    TransactionManager txm;

    @Test
    void refcountParentChange() {
        final JObjectKey PARENT_1_KEY = JObjectKey.of("refcountParentChange_parent1");
        final JObjectKey PARENT_2_KEY = JObjectKey.of("refcountParentChange_parent2");
        final JObjectKey CHILD_KEY = JObjectKey.of("refcountParentChange_child");

        txm.run(() -> {
            curTx.put((new TestRefcount(PARENT_1_KEY)).withFrozen(true));
            curTx.put((new TestRefcount(PARENT_2_KEY)).withFrozen(true));
        });

        txm.run(() -> {
            curTx.put((new TestRefcount(CHILD_KEY)).withFrozen(false));
            curTx.put(curTx.get(TestRefcount.class, PARENT_1_KEY).get().withKids(HashTreePSet.<JObjectKey>empty().plus(CHILD_KEY)));
        });

        txm.run(() -> {
            var kid = curTx.get(TestRefcount.class, CHILD_KEY).get();
            Assertions.assertIterableEquals(List.of(PARENT_1_KEY), kid.refsFrom().stream().map(JDataRef::obj).toList());
        });

        txm.run(() -> {
            curTx.put(curTx.get(TestRefcount.class, PARENT_1_KEY).get().withKids(HashTreePSet.<JObjectKey>empty().minus(CHILD_KEY)));
            curTx.put(curTx.get(TestRefcount.class, PARENT_2_KEY).get().withKids(HashTreePSet.<JObjectKey>empty().plus(CHILD_KEY)));
        });

        txm.run(() -> {
            Assertions.assertIterableEquals(List.of(PARENT_2_KEY), curTx.get(TestRefcount.class, CHILD_KEY).get().refsFrom().stream().map(JDataRef::obj).toList());
        });
    }

}
