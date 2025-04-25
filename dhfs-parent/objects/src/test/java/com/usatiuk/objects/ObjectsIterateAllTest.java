package com.usatiuk.objects;

import com.usatiuk.objects.data.Parent;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

class ObjectsIterateAllTestProfiles {
    public static class ObjectsIterateAllTestProfile extends TempDataProfile {

    }
}

@QuarkusTest
@TestProfile(ObjectsIterateAllTestProfiles.ObjectsIterateAllTestProfile.class)
public class ObjectsIterateAllTest {
    @Inject
    TransactionManager txm;

    @Inject
    Transaction curTx;

    @Test
    void testBegin() {
        var newParent = new Parent(JObjectKey.of("IterateAllBegin1"), "John1");
        var newParent2 = new Parent(JObjectKey.of("IterateAllBegin2"), "John2");
        var newParent3 = new Parent(JObjectKey.of("IterateAllBegin3"), "John3");
        txm.run(() -> {
            curTx.put(newParent);
            curTx.put(newParent2);
            curTx.put(newParent3);
        });

        txm.run(() -> {
            try (var it = curTx.getIterator(JObjectKey.first())) {
                Just.checkIterator(it, Stream.<JData>of(newParent, newParent2, newParent3).map(p -> Pair.of(p.key(), p)).toList());
            }
        });

        txm.run(() -> {
            try (var it = curTx.getIterator(JObjectKey.last()).reversed()) {
                Just.checkIterator(it, Stream.<JData>of(newParent3, newParent2, newParent).map(p -> Pair.of(p.key(), p)).toList());
            }
        });
    }

}
