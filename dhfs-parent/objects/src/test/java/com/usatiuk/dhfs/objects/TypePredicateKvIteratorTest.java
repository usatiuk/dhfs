package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pcollections.TreePMap;

import java.util.List;

public class TypePredicateKvIteratorTest {

//    @Test
//    public void simpleTest() {
//        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6);
//        var pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.GT, 1),
//                IteratorStart.GE, 1, v -> (v % 2 == 0) ? v : null);
//        var expected = List.of(Pair.of(4, 6));
//        for (var pair : expected) {
//            Assertions.assertTrue(pit.hasNext());
//            Assertions.assertEquals(pair, pit.next());
//        }
//    }
//
//    @Test
//    public void ltTest() {
//        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6);
//        var pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 4),
//                IteratorStart.LT, 4, v -> (v % 2 == 0) ? v : null);
//        var expected = List.of(Pair.of(4, 6));
//        for (var pair : expected) {
//            Assertions.assertTrue(pit.hasNext());
//            Assertions.assertEquals(pair, pit.next());
//        }
//        Assertions.assertFalse(pit.hasNext());
//    }
//
//    @Test
//    public void ltTest2() {
//        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6);
//        var pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 1),
//                IteratorStart.LT, 1, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 2),
//                IteratorStart.LT, 2, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 4),
//                IteratorStart.LT, 4, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LE, 4),
//                IteratorStart.LE, 4, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6));
//        Assertions.assertFalse(pit.hasNext());
//    }
//
//    @Test
//    public void ltTest3() {
//        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6).plus(5, 7).plus(6, 8);
//        var pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 4),
//                IteratorStart.LT, 4, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6), Pair.of(6, 8));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 5),
//                IteratorStart.LT, 5, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6), Pair.of(6, 8));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 6),
//                IteratorStart.LT, 6, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6), Pair.of(6, 8));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 7),
//                IteratorStart.LT, 7, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(6, 8));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 8),
//                IteratorStart.LT, 8, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(6, 8));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LE, 6),
//                IteratorStart.LE, 6, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(6, 8));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 6),
//                IteratorStart.LT, 6, v -> (v % 2 == 0) ? v : null);
//        Assertions.assertTrue(pit.hasNext());
//        Assertions.assertEquals(4, pit.peekNextKey());
//        Assertions.assertFalse(pit.hasPrev());
//        Assertions.assertEquals(4, pit.peekNextKey());
//        Assertions.assertFalse(pit.hasPrev());
//        Assertions.assertEquals(Pair.of(4, 6), pit.next());
//        Assertions.assertTrue(pit.hasNext());
//        Assertions.assertEquals(6, pit.peekNextKey());
//        Assertions.assertEquals(4, pit.peekPrevKey());
//        Assertions.assertEquals(6, pit.peekNextKey());
//        Assertions.assertEquals(4, pit.peekPrevKey());
//    }
//
//    @Test
//    public void itTest4() {
//        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6).plus(5, 8).plus(6, 10);
//        var pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 4),
//                IteratorStart.LT, 4, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6), Pair.of(5, 8), Pair.of(6, 10));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 5),
//                IteratorStart.LT, 5, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(4, 6), Pair.of(5, 8), Pair.of(6, 10));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 6),
//                IteratorStart.LT, 6, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(5, 8), Pair.of(6, 10));
//        Assertions.assertFalse(pit.hasNext());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 7),
//                IteratorStart.LT, 7, v -> (v % 2 == 0) ? v : null);
//        Just.checkIterator(pit, Pair.of(6, 10));
//        Assertions.assertFalse(pit.hasNext());
//        Assertions.assertTrue(pit.hasPrev());
//        Assertions.assertEquals(6, pit.peekPrevKey());
//        Assertions.assertEquals(Pair.of(6, 10), pit.prev());
//        Assertions.assertTrue(pit.hasNext());
//        Assertions.assertEquals(6, pit.peekNextKey());
//
//        pit = new TypePredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 6),
//                IteratorStart.LT, 6, v -> (v % 2 == 0) ? v : null);
//        Assertions.assertTrue(pit.hasNext());
//        Assertions.assertEquals(5, pit.peekNextKey());
//        Assertions.assertTrue(pit.hasPrev());
//        Assertions.assertEquals(4, pit.peekPrevKey());
//        Assertions.assertEquals(5, pit.peekNextKey());
//        Assertions.assertEquals(4, pit.peekPrevKey());
//        Assertions.assertEquals(Pair.of(5, 8), pit.next());
//        Assertions.assertTrue(pit.hasNext());
//        Assertions.assertEquals(6, pit.peekNextKey());
//        Assertions.assertEquals(5, pit.peekPrevKey());
//        Assertions.assertEquals(6, pit.peekNextKey());
//        Assertions.assertEquals(5, pit.peekPrevKey());
//    }

//    @Test
//    public void reverseTest() {
//        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6);
//        var pit = new PredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 4),
//                IteratorStart.LT, 4, v -> (v % 2 == 0) ? v : null);
//
//    }
}
