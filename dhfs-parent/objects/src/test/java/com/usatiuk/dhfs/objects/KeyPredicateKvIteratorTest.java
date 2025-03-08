package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pcollections.TreePMap;

import java.util.List;

public class KeyPredicateKvIteratorTest {

    @Test
    public void simpleTest() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(3, 3).plus(5, 5).plus(6, 6);
        var pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.GT, 3),
                IteratorStart.GE, 3, v -> (v % 2 == 0));
        var expected = List.of(Pair.of(6, 6));
        for (var pair : expected) {
            Assertions.assertTrue(pit.hasNext());
            Assertions.assertEquals(pair, pit.next());
        }
    }

    @Test
    public void ltTest() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(3, 3).plus(5, 5).plus(6, 6);
        var pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 5),
                IteratorStart.LT, 5, v -> (v % 2 == 0));
        var expected = List.of(Pair.of(6, 6));
        for (var pair : expected) {
            Assertions.assertTrue(pit.hasNext());
            Assertions.assertEquals(pair, pit.next());
        }
        Assertions.assertFalse(pit.hasNext());
    }

    @Test
    public void ltTest2() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(3, 3).plus(5, 5).plus(6, 6);
        var pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 3),
                IteratorStart.LT, 2, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 4),
                IteratorStart.LT, 4, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 5),
                IteratorStart.LT, 5, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LE, 5),
                IteratorStart.LE, 5, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6));
        Assertions.assertFalse(pit.hasNext());
    }

    @Test
    public void ltTest3() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(3, 3).plus(5, 5).plus(6, 6).plus(7, 7).plus(8, 8);
        var pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 5),
                IteratorStart.LT, 5, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 5),
                IteratorStart.LT, 5, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 6),
                IteratorStart.LT, 6, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 7),
                IteratorStart.LT, 7, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 8),
                IteratorStart.LT, 8, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LE, 6),
                IteratorStart.LE, 6, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 6),
                IteratorStart.LT, 6, v -> (v % 2 == 0));
        Assertions.assertTrue(pit.hasNext());
        Assertions.assertEquals(6, pit.peekNextKey());
        Assertions.assertFalse(pit.hasPrev());
        Assertions.assertEquals(6, pit.peekNextKey());
        Assertions.assertFalse(pit.hasPrev());
        Assertions.assertEquals(Pair.of(6, 6), pit.next());
        Assertions.assertTrue(pit.hasNext());
        Assertions.assertEquals(8, pit.peekNextKey());
        Assertions.assertEquals(6, pit.peekPrevKey());
        Assertions.assertEquals(8, pit.peekNextKey());
        Assertions.assertEquals(6, pit.peekPrevKey());
    }

    @Test
    public void itTest4() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(3, 3).plus(5, 5).plus(6, 6).plus(8, 8).plus(10, 10);
        var pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 5),
                IteratorStart.LT, 5, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8), Pair.of(10, 10));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 5),
                IteratorStart.LT, 5, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8), Pair.of(10, 10));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 6),
                IteratorStart.LT, 6, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8), Pair.of(10, 10));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 7),
                IteratorStart.LT, 7, v -> (v % 2 == 0));
        Just.checkIterator(pit, Pair.of(6, 6), Pair.of(8, 8), Pair.of(10, 10));
        Assertions.assertFalse(pit.hasNext());

        pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 6),
                IteratorStart.LT, 6, v -> (v % 2 == 0));
        Assertions.assertTrue(pit.hasNext());
        Assertions.assertEquals(6, pit.peekNextKey());
        Assertions.assertFalse(pit.hasPrev());
        Assertions.assertEquals(6, pit.peekNextKey());
        Assertions.assertEquals(Pair.of(6, 6), pit.next());
        Assertions.assertTrue(pit.hasNext());
        Assertions.assertEquals(8, pit.peekNextKey());
        Assertions.assertEquals(6, pit.peekPrevKey());
        Assertions.assertEquals(8, pit.peekNextKey());
        Assertions.assertEquals(6, pit.peekPrevKey());
    }

//    @Test
//    public void reverseTest() {
//        var source1 = TreePMap.<Integer, Integer>empty().plus(3, 3).plus(5, 5).plus(6, 6);
//        var pit = new KeyPredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 4),
//                IteratorStart.LT, 4, v -> (v % 2 == 0) );
//
//    }
}
