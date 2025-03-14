package com.usatiuk.dhfs.objects.jmap;

import com.usatiuk.dhfs.objects.iterators.CloseableKvIterator;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.iterators.IteratorStart;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Optional;

@ApplicationScoped
public class JMapHelper {
    @Inject
    Transaction curTx;

    static <K extends JMapKey & Comparable<K>> JObjectKey makePrefix(JObjectKey holder) {
        return JObjectKey.of(holder.name() + "/");
    }

    static <K extends JMapKey & Comparable<K>> JObjectKey makeKey(JObjectKey holder, K key) {
        return JObjectKey.of(makePrefix(holder).name() + key.toString());
    }

    public <K extends JMapKey & Comparable<K>> CloseableKvIterator<K, JMapEntry<K>> getIterator(JMapHolder<K> holder, IteratorStart start, K key) {
        return new JMapIterator<>(curTx.getIterator(start, makeKey(holder.key(), key)), holder);
    }

    public <K extends JMapKey & Comparable<K>> CloseableKvIterator<K, JMapEntry<K>> getIterator(JMapHolder<K> holder, K key) {
        return getIterator(holder, IteratorStart.GE, key);
    }

    public <K extends JMapKey & Comparable<K>> CloseableKvIterator<K, JMapEntry<K>> getIterator(JMapHolder<K> holder, IteratorStart start) {
        return new JMapIterator<>(curTx.getIterator(start, makePrefix(holder.key())), holder);
    }

    public <K extends JMapKey & Comparable<K>> void put(JMapHolder<K> holder, K key, JObjectKey ref) {
        curTx.put(new JMapEntry<>(holder.key(), key, ref));
    }

    public <K extends JMapKey & Comparable<K>> Optional<JMapEntry<K>> get(JMapHolder<K> holder, K key) {
        // TODO:
        return curTx.get(JMapEntry.class, makeKey(holder.key(), key)).map(e -> (JMapEntry<K>) e);
    }

    public <K extends JMapKey & Comparable<K>> void delete(JMapHolder<K> holder, K key) {
        curTx.delete(makeKey(holder.key(), key));
    }
}
