package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pcollections.TreePMap;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MergingKvIteratorTest {

    private class SimpleIteratorWrapper<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
        private final Iterator<Pair<K, V>> _iterator;
        private Pair<K, V> _next;

        public SimpleIteratorWrapper(Iterator<Pair<K, V>> iterator) {
            _iterator = iterator;
            fillNext();
        }

        private void fillNext() {
            while (_iterator.hasNext() && _next == null) {
                _next = _iterator.next();
            }
        }

        @Override
        public K peekNextKey() {
            if (_next == null) {
                throw new NoSuchElementException();
            }
            return _next.getKey();
        }

        @Override
        public void skip() {
            if (_next == null) {
                throw new NoSuchElementException();
            }
            _next = null;
            fillNext();
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            return _next != null;
        }

        @Override
        public Pair<K, V> next() {
            if (_next == null) {
                throw new NoSuchElementException("No more elements");
            }
            var ret = _next;
            _next = null;
            fillNext();
            return ret;
        }
    }

    @Test
    public void testTestIterator() {
        var list = List.of(Pair.of(1, 2), Pair.of(3, 4), Pair.of(5, 6));
        var iterator = new SimpleIteratorWrapper<>(list.iterator());
        var realIterator = list.iterator();
        while (realIterator.hasNext()) {
            Assertions.assertTrue(iterator.hasNext());
            Assertions.assertEquals(realIterator.next(), iterator.next());
        }
        Assertions.assertFalse(iterator.hasNext());

        var emptyList = List.<Pair<Integer, Integer>>of();
        var emptyIterator = new SimpleIteratorWrapper<>(emptyList.iterator());
        Assertions.assertFalse(emptyIterator.hasNext());
    }

    @Test
    public void testSimple() {
        var source1 = List.of(Pair.of(1, 2), Pair.of(3, 4), Pair.of(5, 6)).iterator();
        var source2 = List.of(Pair.of(2, 3), Pair.of(4, 5), Pair.of(6, 7)).iterator();
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.GE, 0, (a, b) -> new SimpleIteratorWrapper<>(source1), (a, b) -> new SimpleIteratorWrapper<>(source2));
        var expected = List.of(Pair.of(1, 2), Pair.of(2, 3), Pair.of(3, 4), Pair.of(4, 5), Pair.of(5, 6), Pair.of(6, 7));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
    }

    @Test
    public void testPriority() {
        var source1 = List.of(Pair.of(1, 2), Pair.of(2, 4), Pair.of(5, 6));
        var source2 = List.of(Pair.of(1, 3), Pair.of(2, 5), Pair.of(5, 7));
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.GE, 0, (a, b) -> new SimpleIteratorWrapper<>(source1.iterator()), (a, b) -> new SimpleIteratorWrapper<>(source2.iterator()));
        var expected = List.of(Pair.of(1, 2), Pair.of(2, 4), Pair.of(5, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.GE, 0, (a, b) -> new SimpleIteratorWrapper<>(source2.iterator()), (a, b) -> new SimpleIteratorWrapper<>(source1.iterator()));
        var expected2 = List.of(Pair.of(1, 3), Pair.of(2, 5), Pair.of(5, 7));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
    }

    @Test
    public void testPriority2() {
        var source1 = List.of(Pair.of(2, 4), Pair.of(5, 6));
        var source2 = List.of(Pair.of(1, 3), Pair.of(2, 5));
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.GE, 0, (a, b) -> new SimpleIteratorWrapper<>(source1.iterator()), (a, b) -> new SimpleIteratorWrapper<>(source2.iterator()));
        var expected = List.of(Pair.of(1, 3), Pair.of(2, 4), Pair.of(5, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.GE, 0, (a, b) -> new SimpleIteratorWrapper<>(source2.iterator()), (a, b) -> new SimpleIteratorWrapper<>(source1.iterator()));
        var expected2 = List.of(Pair.of(1, 3), Pair.of(2, 5), Pair.of(5, 6));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
    }

    @Test
    public void testPriorityLe() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(2, 4).plus(5, 6);
        var source2 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(2, 5);
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        var expected = List.of(Pair.of(5, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());
        Just.checkIterator(mergingIterator.reversed(), Pair.of(5, 6), Pair.of(2, 4), Pair.of(1, 3));
        Assertions.assertFalse(mergingIterator.reversed().hasNext());
        Just.checkIterator(mergingIterator, Pair.of(1,3), Pair.of(2, 4), Pair.of(5, 6));
        Assertions.assertFalse(mergingIterator.hasNext());


        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK));
        var expected2 = List.of(Pair.of(5, 6));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
        Just.checkIterator(mergingIterator2.reversed(), Pair.of(5, 6), Pair.of(2, 5), Pair.of(1, 3));
        Assertions.assertFalse(mergingIterator2.reversed().hasNext());
        Just.checkIterator(mergingIterator2, Pair.of(1,3), Pair.of(2, 5), Pair.of(5, 6));
        Assertions.assertFalse(mergingIterator2.hasNext());

        var mergingIterator3 = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        Assertions.assertEquals(5, mergingIterator3.peekNextKey());
        Assertions.assertEquals(2, mergingIterator3.peekPrevKey());
        Assertions.assertEquals(5, mergingIterator3.peekNextKey());
        Assertions.assertEquals(2, mergingIterator3.peekPrevKey());
    }

    @Test
    public void testPriorityLe2() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(2, 4).plus(5, 6);
        var source2 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(2, 5).plus(3, 4);
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        var expected = List.of(Pair.of(5, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());
    }

    @Test
    public void testPriorityLe3() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(2, 4).plus(5, 6);
        var source2 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(2, 5).plus(6, 8);
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        var expected = List.of(Pair.of(5, 6), Pair.of(6, 8));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());
        Just.checkIterator(mergingIterator.reversed(), Pair.of(6, 8), Pair.of(5, 6), Pair.of(2, 4), Pair.of(1, 3));
        Assertions.assertFalse(mergingIterator.reversed().hasNext());
        Just.checkIterator(mergingIterator, Pair.of(1, 3), Pair.of(2, 4), Pair.of(5, 6), Pair.of(6, 8));
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK));
        var expected2 = List.of(Pair.of(5, 6), Pair.of(6, 8));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());

        var mergingIterator3 = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        Assertions.assertEquals(5, mergingIterator3.peekNextKey());
        Assertions.assertEquals(2, mergingIterator3.peekPrevKey());
        Assertions.assertEquals(5, mergingIterator3.peekNextKey());
        Assertions.assertEquals(2, mergingIterator3.peekPrevKey());
    }

    @Test
    public void testPriorityLe4() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(6, 7);
        var source2 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(2, 5).plus(3, 4);
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        var expected = List.of(Pair.of(3, 4), Pair.of(6, 7));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK));
        var expected2 = List.of(Pair.of(3, 4), Pair.of(6, 7));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
    }

    @Test
    public void testPriorityLe5() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 2).plus(6, 7);
        var source2 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(2, 5).plus(3, 4);
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        var expected = List.of(Pair.of(3, 4), Pair.of(6, 7));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK));
        var expected2 = List.of(Pair.of(3, 4), Pair.of(6, 7));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
    }

    @Test
    public void testPriorityLe6() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(2, 5).plus(3, 4);
        var source2 = TreePMap.<Integer, Integer>empty().plus(4, 6);
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        var expected = List.of(Pair.of(4, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.LE, 5, (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK));
        var expected2 = List.of(Pair.of(4, 6));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
    }

    @Test
    public void testPriorityLe7() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6);
        var source2 = TreePMap.<Integer, Integer>empty().plus(1, 4).plus(3, 5).plus(4, 6);
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.LE, 2, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        var expected = List.of(Pair.of(1, 3), Pair.of(3, 5), Pair.of(4, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());
        Just.checkIterator(mergingIterator.reversed(), Pair.of(4, 6), Pair.of(3, 5), Pair.of(1, 3));
        Just.checkIterator(mergingIterator, Pair.of(1, 3), Pair.of(3, 5), Pair.of(4, 6));

        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.LE, 2, (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK));
        var expected2 = List.of(Pair.of(1, 4), Pair.of(3, 5), Pair.of(4, 6));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
    }

    @Test
    public void testPriorityLt() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(2, 4).plus(5, 6);
        var source2 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(2, 5);
        var mergingIterator = new MergingKvIterator<>("test", IteratorStart.LT, 5, (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK));
        var expected = List.of(Pair.of(2, 4), Pair.of(5, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", IteratorStart.LT, 5, (mS, mK) -> new NavigableMapKvIterator<>(source2, mS, mK), (mS, mK) -> new NavigableMapKvIterator<>(source1, mS, mK));
        var expected2 = List.of(Pair.of(2, 5), Pair.of(5, 6));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
    }
}
