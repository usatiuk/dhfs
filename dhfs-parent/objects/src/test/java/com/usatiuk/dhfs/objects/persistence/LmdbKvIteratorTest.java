package com.usatiuk.dhfs.objects.persistence;


import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.Just;
import com.usatiuk.dhfs.objects.TempDataProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.util.List;

class Profiles {
    public static class LmdbKvIteratorTestProfile extends TempDataProfile {
    }
}

@QuarkusTest
@TestProfile(Profiles.LmdbKvIteratorTestProfile.class)
public class LmdbKvIteratorTest {

    @Inject
    LmdbObjectPersistentStore store;

    @RepeatedTest(100)
    public void iteratorTest1() {
        store.commitTx(
                new TxManifestRaw(
                        List.of(Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})),
                                Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})),
                                Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4}))),
                        List.of()
                ), -1, Runnable::run
        );

        var iterator = store.getIterator(IteratorStart.LE, JObjectKey.of(Long.toString(3)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.LE, JObjectKey.of(Long.toString(2)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.GE, JObjectKey.of(Long.toString(2)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.GT, JObjectKey.of(Long.toString(2)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.LT, JObjectKey.of(Long.toString(3)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.LT, JObjectKey.of(Long.toString(2)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.LT, JObjectKey.of(Long.toString(1)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.LE, JObjectKey.of(Long.toString(1)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.GT, JObjectKey.of(Long.toString(3)));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.GT, JObjectKey.of(Long.toString(4)));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.LE, JObjectKey.of(Long.toString(0)));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertFalse(iterator.hasNext());
        iterator.close();

        iterator = store.getIterator(IteratorStart.GE, JObjectKey.of(Long.toString(2)));
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals(JObjectKey.of(Long.toString(2)), iterator.peekNextKey());
        Assertions.assertEquals(JObjectKey.of(Long.toString(1)), iterator.peekPrevKey());
        Assertions.assertEquals(JObjectKey.of(Long.toString(2)), iterator.peekNextKey());
        Assertions.assertEquals(JObjectKey.of(Long.toString(1)), iterator.peekPrevKey());
        Just.checkIterator(iterator.reversed(), Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})));
        Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
        Assertions.assertEquals(Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})), iterator.prev());
        Assertions.assertEquals(Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), iterator.prev());
        Assertions.assertEquals(Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), iterator.next());
        iterator.close();

        store.commitTx(new TxManifestRaw(
                        List.of(),
                        List.of(JObjectKey.of(Long.toString(1)), JObjectKey.of(Long.toString(2)), JObjectKey.of(Long.toString(3)))
                ),
                -1, Runnable::run
        );
    }
}
