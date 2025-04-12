//package com.usatiuk.objects.stores;
//
//
//import com.google.protobuf.ByteString;
//import com.usatiuk.objects.JObjectKey;
//import com.usatiuk.objects.Just;
//import com.usatiuk.objects.TempDataProfile;
//import com.usatiuk.objects.iterators.IteratorStart;
//import io.quarkus.test.junit.QuarkusTest;
//import io.quarkus.test.junit.TestProfile;
//import jakarta.inject.Inject;
//import org.apache.commons.lang3.tuple.Pair;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.RepeatedTest;
//
//import java.util.List;
//
//class Profiles {
//    public static class LmdbKvIteratorTestProfile extends TempDataProfile {
//    }
//}
//
//@QuarkusTest
//@TestProfile(Profiles.LmdbKvIteratorTestProfile.class)
//public class LmdbKvIteratorTest {
//
//    @Inject
//    LmdbObjectPersistentStore store;
//
//    long getNextTxId() {
//        try (var s = store.getSnapshot()) {
//            return s.id() + 1;
//        }
//    }
//
//    @RepeatedTest(100)
//    public void iteratorTest1() {
//        store.prepareTx(
//                new TxManifestRaw(
//                        List.of(Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})),
//                                Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})),
//                                Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4}))),
//                        List.of()
//                ), getNextTxId()
//        ).run();
//
//        try (var snapshot = store.getSnapshot()) {
//            var iterator = snapshot.getIterator(IteratorStart.GE, JObjectKey.of(""));
//            Just.checkIterator(iterator, List.of(Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})),
//                    Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})),
//                    Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4}))));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.LE, JObjectKey.of(Long.toString(3)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.LE, JObjectKey.of(Long.toString(2)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.GE, JObjectKey.of(Long.toString(2)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.GT, JObjectKey.of(Long.toString(2)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.LT, JObjectKey.of(Long.toString(3)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.LT, JObjectKey.of(Long.toString(2)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.LT, JObjectKey.of(Long.toString(1)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.LE, JObjectKey.of(Long.toString(1)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.GT, JObjectKey.of(Long.toString(3)));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.GT, JObjectKey.of(Long.toString(4)));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.LE, JObjectKey.of(Long.toString(0)));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertFalse(iterator.hasNext());
//            iterator.close();
//
//            iterator = snapshot.getIterator(IteratorStart.GE, JObjectKey.of(Long.toString(2)));
//            Assertions.assertTrue(iterator.hasNext());
//            Assertions.assertEquals(JObjectKey.of(Long.toString(2)), iterator.peekNextKey());
//            Assertions.assertEquals(JObjectKey.of(Long.toString(1)), iterator.peekPrevKey());
//            Assertions.assertEquals(JObjectKey.of(Long.toString(2)), iterator.peekNextKey());
//            Assertions.assertEquals(JObjectKey.of(Long.toString(1)), iterator.peekPrevKey());
//            Just.checkIterator(iterator.reversed(), Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})));
//            Just.checkIterator(iterator, Pair.of(JObjectKey.of(Long.toString(1)), ByteString.copyFrom(new byte[]{2})), Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})));
//            Assertions.assertEquals(Pair.of(JObjectKey.of(Long.toString(3)), ByteString.copyFrom(new byte[]{4})), iterator.prev());
//            Assertions.assertEquals(Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), iterator.prev());
//            Assertions.assertEquals(Pair.of(JObjectKey.of(Long.toString(2)), ByteString.copyFrom(new byte[]{3})), iterator.next());
//            iterator.close();
//        }
//
//        store.prepareTx(new TxManifestRaw(
//                        List.of(),
//                        List.of(JObjectKey.of(Long.toString(1)), JObjectKey.of(Long.toString(2)), JObjectKey.of(Long.toString(3)))
//                ),
//                getNextTxId()
//        ).run();
//    }
//}
