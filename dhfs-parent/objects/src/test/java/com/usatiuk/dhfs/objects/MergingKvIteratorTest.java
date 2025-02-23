package com.usatiuk.dhfs.objects;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        var mergingIterator = new MergingKvIterator<>("test", new SimpleIteratorWrapper<>(source1), new SimpleIteratorWrapper<>(source2));
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
        var mergingIterator = new MergingKvIterator<>("test", new SimpleIteratorWrapper<>(source1.iterator()), new SimpleIteratorWrapper<>(source2.iterator()));
        var expected = List.of(Pair.of(1, 2), Pair.of(2, 4), Pair.of(5, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", new SimpleIteratorWrapper<>(source2.iterator()), new SimpleIteratorWrapper<>(source1.iterator()));
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
        var mergingIterator = new MergingKvIterator<>("test", new SimpleIteratorWrapper<>(source1.iterator()), new SimpleIteratorWrapper<>(source2.iterator()));
        var expected = List.of(Pair.of(1, 3), Pair.of(2, 4), Pair.of(5, 6));
        for (var pair : expected) {
            Assertions.assertTrue(mergingIterator.hasNext());
            Assertions.assertEquals(pair, mergingIterator.next());
        }
        Assertions.assertFalse(mergingIterator.hasNext());

        var mergingIterator2 = new MergingKvIterator<>("test", new SimpleIteratorWrapper<>(source2.iterator()), new SimpleIteratorWrapper<>(source1.iterator()));
        var expected2 = List.of(Pair.of(1, 3), Pair.of(2, 5), Pair.of(5, 6));
        for (var pair : expected2) {
            Assertions.assertTrue(mergingIterator2.hasNext());
            Assertions.assertEquals(pair, mergingIterator2.next());
        }
        Assertions.assertFalse(mergingIterator2.hasNext());
    }
}
