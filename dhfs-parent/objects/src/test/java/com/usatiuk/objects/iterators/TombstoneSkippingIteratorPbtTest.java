package com.usatiuk.objects.iterators;

import net.jqwik.api.*;
import net.jqwik.api.state.Action;
import net.jqwik.api.state.ActionChain;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TombstoneSkippingIteratorPbtTest {
    @Property
    public void checkMergingIterator(@ForAll("actions") ActionChain<MergingIteratorModel> actions) {
        actions.run();
    }

    @Provide
    Arbitrary<ActionChain<MergingIteratorModel>> actions(@ForAll("lists") List<List<Map.Entry<Integer, MaybeTombstone<Integer>>>> list,
                                                         @ForAll IteratorStart iteratorStart, @ForAll("startKey") Integer startKey) {
        return ActionChain.startWith(() -> new MergingIteratorModel(list, iteratorStart, startKey))
                .withAction(new NextAction())
                .withAction(new PeekNextKeyAction())
                .withAction(new SkipAction())
                .withAction(new PeekPrevKeyAction())
                .withAction(new SkipPrevAction())
                .withAction(new PrevAction())
                .withAction(new HasNextAction())
                .withAction(new HasPrevAction());
    }

    @Provide
    Arbitrary<List<List<Map.Entry<Integer, MaybeTombstone<Integer>>>>> lists() {
        return Arbitraries.entries(Arbitraries.integers().between(-50, 50),
                        Arbitraries.integers().between(-50, 50).flatMap(i -> Arbitraries.of(true, false).<MaybeTombstone<Integer>>flatMap(
                                b -> b ? Arbitraries.just(new DataWrapper<Integer>(i)) : Arbitraries.just(new TombstoneImpl<>())
                        ))
                )
                .list().uniqueElements(Map.Entry::getKey).ofMinSize(0).ofMaxSize(20)
                .list().ofMinSize(1).ofMaxSize(5);
    }

    @Provide
    Arbitrary<Integer> startKey() {
        return Arbitraries.integers().between(-51, 51);
    }

    static class MergingIteratorModel implements CloseableKvIterator<Integer, Integer> {
        private final CloseableKvIterator<Integer, Integer> mergedIterator;
        private final CloseableKvIterator<Integer, Integer> mergingIterator;

        private MergingIteratorModel(List<List<Map.Entry<Integer, MaybeTombstone<Integer>>>> pairs, IteratorStart startType, Integer startKey) {
            TreeMap<Integer, MaybeTombstone<Integer>> perfectMergedTombstones = new TreeMap<>();
            for (List<Map.Entry<Integer, MaybeTombstone<Integer>>> list : pairs) {
                for (Map.Entry<Integer, MaybeTombstone<Integer>> pair : list) {
                    perfectMergedTombstones.putIfAbsent(pair.getKey(), pair.getValue());
                }
            }
            TreeMap<Integer, Integer> perfectMerged = new TreeMap<>();
            for (var e : perfectMergedTombstones.entrySet()) {
                if (e.getValue() instanceof Data<Integer> data)
                    perfectMerged.put(e.getKey(), data.value());
            }


            mergedIterator = new NavigableMapKvIterator<>(perfectMerged, startType, startKey);
            mergingIterator = new TombstoneSkippingIterator<>(startType, startKey, pairs.stream().<CloseableKvIterator<Integer, MaybeTombstone<Integer>>>map(
                    list -> new NavigableMapKvIterator<Integer, MaybeTombstone<Integer>>(new TreeMap<Integer, MaybeTombstone<Integer>>(Map.ofEntries(list.toArray(Map.Entry[]::new))), startType, startKey)
            ).toList());
        }

        @Override
        public Integer peekNextKey() {
            var mergedKey = mergedIterator.peekNextKey();
            var mergingKey = mergingIterator.peekNextKey();
            Assertions.assertEquals(mergedKey, mergingKey);
            return mergedKey;
        }

        @Override
        public void skip() {
            mergedIterator.skip();
            mergingIterator.skip();
        }

        @Override
        public Integer peekPrevKey() {
            var mergedKey = mergedIterator.peekPrevKey();
            var mergingKey = mergingIterator.peekPrevKey();
            Assertions.assertEquals(mergedKey, mergingKey);
            return mergedKey;
        }

        @Override
        public Pair<Integer, Integer> prev() {
            var mergedKey = mergedIterator.prev();
            var mergingKey = mergingIterator.prev();
            Assertions.assertEquals(mergedKey, mergingKey);
            return mergedKey;
        }

        @Override
        public boolean hasPrev() {
            var mergedKey = mergedIterator.hasPrev();
            var mergingKey = mergingIterator.hasPrev();
            Assertions.assertEquals(mergedKey, mergingKey);
            return mergedKey;
        }

        @Override
        public void skipPrev() {
            mergedIterator.skipPrev();
            mergingIterator.skipPrev();
        }

        @Override
        public void close() {
            mergedIterator.close();
            mergingIterator.close();
        }

        @Override
        public boolean hasNext() {
            var mergedKey = mergedIterator.hasNext();
            var mergingKey = mergingIterator.hasNext();
            Assertions.assertEquals(mergedKey, mergingKey);
            return mergedKey;
        }

        @Override
        public Pair<Integer, Integer> next() {
            var mergedKey = mergedIterator.next();
            var mergingKey = mergingIterator.next();
            Assertions.assertEquals(mergedKey, mergingKey);
            return mergedKey;
        }
    }

    static class PeekNextKeyAction extends Action.JustMutate<MergingIteratorModel> {
        @Override
        public void mutate(MergingIteratorModel state) {
            state.peekNextKey();
        }

        @Override
        public boolean precondition(MergingIteratorModel state) {
            return state.hasNext();
        }

        @Override
        public String description() {
            return "Peek next key";
        }
    }

    static class SkipAction extends Action.JustMutate<MergingIteratorModel> {
        @Override
        public void mutate(MergingIteratorModel state) {
            state.skip();
        }

        @Override
        public boolean precondition(MergingIteratorModel state) {
            return state.hasNext();
        }

        @Override
        public String description() {
            return "Skip next key";
        }
    }

    static class PeekPrevKeyAction extends Action.JustMutate<MergingIteratorModel> {
        @Override
        public void mutate(MergingIteratorModel state) {
            state.peekPrevKey();
        }

        @Override
        public boolean precondition(MergingIteratorModel state) {
            return state.hasPrev();
        }

        @Override
        public String description() {
            return "Peek prev key";
        }
    }

    static class SkipPrevAction extends Action.JustMutate<MergingIteratorModel> {
        @Override
        public void mutate(MergingIteratorModel state) {
            state.skipPrev();
        }

        @Override
        public boolean precondition(MergingIteratorModel state) {
            return state.hasPrev();
        }

        @Override
        public String description() {
            return "Skip prev key";
        }
    }

    static class PrevAction extends Action.JustMutate<MergingIteratorModel> {
        @Override
        public void mutate(MergingIteratorModel state) {
            state.prev();
        }

        @Override
        public boolean precondition(MergingIteratorModel state) {
            return state.hasPrev();
        }

        @Override
        public String description() {
            return "Prev key";
        }
    }

    static class NextAction extends Action.JustMutate<MergingIteratorModel> {
        @Override
        public void mutate(MergingIteratorModel state) {
            state.next();
        }

        @Override
        public boolean precondition(MergingIteratorModel state) {
            return state.hasNext();
        }

        @Override
        public String description() {
            return "Next key";
        }
    }

    static class HasNextAction extends Action.JustMutate<MergingIteratorModel> {
        @Override
        public void mutate(MergingIteratorModel state) {
            state.hasNext();
        }

        @Override
        public boolean precondition(MergingIteratorModel state) {
            return true;
        }

        @Override
        public String description() {
            return "Has next key";
        }
    }

    static class HasPrevAction extends Action.JustMutate<MergingIteratorModel> {
        @Override
        public void mutate(MergingIteratorModel state) {
            state.hasPrev();
        }

        @Override
        public boolean precondition(MergingIteratorModel state) {
            return true;
        }

        @Override
        public String description() {
            return "Has prev key";
        }
    }
}