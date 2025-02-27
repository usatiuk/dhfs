package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pcollections.TreePMap;

import java.util.NavigableMap;

public class NavigableMapKvIteratorTest {
    private final NavigableMap<Integer, Integer> _testMap1 = TreePMap.<Integer, Integer>empty().plus(1, 2).plus(2, 3).plus(3, 4);

    @Test
    void test1() {
        var iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.LE, 3);
        Just.checkIterator(iterator, Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.LE, 2);
        Just.checkIterator(iterator, Pair.of(2, 3), Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.GE, 2);
        Just.checkIterator(iterator, Pair.of(2, 3), Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.GT, 2);
        Just.checkIterator(iterator, Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.LT, 3);
        Just.checkIterator(iterator, Pair.of(2, 3), Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.LT, 2);
        Just.checkIterator(iterator, Pair.of(1, 2), Pair.of(2, 3), Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.LT, 1);
        Just.checkIterator(iterator, Pair.of(1, 2), Pair.of(2, 3), Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.LE, 1);
        Just.checkIterator(iterator, Pair.of(1, 2), Pair.of(2, 3), Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.GT, 3);
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.GT, 4);
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.LE, 0);
        Just.checkIterator(iterator, Pair.of(1, 2), Pair.of(2, 3), Pair.of(3, 4));
        Assertions.assertFalse(iterator.hasNext());

        iterator = new NavigableMapKvIterator<>(_testMap1, IteratorStart.GE, 2);
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals(2, iterator.peekNextKey());
        Assertions.assertEquals(1, iterator.peekPrevKey());
        Assertions.assertEquals(2, iterator.peekNextKey());
        Assertions.assertEquals(1, iterator.peekPrevKey());
        Just.checkIterator(iterator.reversed(), Pair.of(1, 2));
        Just.checkIterator(iterator, Pair.of(1, 2), Pair.of(2, 3), Pair.of(3, 4));
        Assertions.assertEquals(Pair.of(3, 4), iterator.prev());
        Assertions.assertEquals(Pair.of(2, 3), iterator.prev());
        Assertions.assertEquals(Pair.of(2, 3), iterator.next());
    }

}
