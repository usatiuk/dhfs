package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pcollections.TreePMap;

import java.util.List;

public class PredicateKvIteratorTest {

    @Test
    public void simpleTest() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6);
        var pit = new PredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.GT, 1),
                IteratorStart.GE, 1, v -> (v % 2 == 0) ? v : null);
        var expected = List.of(Pair.of(4, 6));
        for (var pair : expected) {
            Assertions.assertTrue(pit.hasNext());
            Assertions.assertEquals(pair, pit.next());
        }
    }

    @Test
    public void ltTest() {
        var source1 = TreePMap.<Integer, Integer>empty().plus(1, 3).plus(3, 5).plus(4, 6);
        var pit = new PredicateKvIterator<>(new NavigableMapKvIterator<>(source1, IteratorStart.LT, 4),
                IteratorStart.LT, 4, v -> (v % 2 == 0) ? v : null);
        var expected = List.of();
        for (var pair : expected) {
            Assertions.assertTrue(pit.hasNext());
            Assertions.assertEquals(pair, pit.next());
        }
        Assertions.assertFalse(pit.hasNext());
    }
}
