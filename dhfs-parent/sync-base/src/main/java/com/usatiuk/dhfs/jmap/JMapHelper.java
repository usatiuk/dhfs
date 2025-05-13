package com.usatiuk.dhfs.jmap;

import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Optional;

// TODO: It's not actually generic right now, only longs are supported essentially

/**
 * Persistent-storage backed ordered map service.
 * Local and remote objects can implement the ${@link JMapHolder} interface, then they can be used with this service
 * to store references to other objects identified by sorded keys of some kind. (for now only longs)
 */
@Singleton
public class JMapHelper {
    @Inject
    Transaction curTx;

    static <K extends JMapKey> JObjectKey makePrefix(JObjectKey holder) {
        return JObjectKey.of(holder.value() + "=");
    }

    static <K extends JMapKey> JObjectKey makeKeyFirst(JObjectKey holder) {
        return JObjectKey.of(holder.value() + "<");
    }

    static <K extends JMapKey> JObjectKey makeKey(JObjectKey holder, K key) {
        return JObjectKey.of(makePrefix(holder).value() + key.toString());
    }

    static <K extends JMapKey> JObjectKey makeKeyLast(JObjectKey holder) {
        return JObjectKey.of(holder.value() + ">");
    }


    /**
     * Get an iterator for the map of a given holder.
     * @param holder the holder of the map
     * @param start the start position of the iterator relative to the key
     * @param key the key to start the iterator from
     * @return an iterator for the map of the given holder
     * @param <K> the type of the key
     */
    public <K extends JMapKey> CloseableKvIterator<K, JMapEntry<K>> getIterator(JMapHolder<K> holder, IteratorStart start, K key) {
        return new JMapIterator<>(curTx.getIterator(start, makeKey(holder.key(), key)), holder);
    }

    /**
     * Get an iterator for the map of a given holder. The iterator starts from the first key.
     * @param holder the holder of the map
     * @return an iterator for the map of the given holder
     * @param <K> the type of the key
     */
    public <K extends JMapKey> CloseableKvIterator<K, JMapEntry<K>> getIterator(JMapHolder<K> holder) {
        return new JMapIterator<>(curTx.getIterator(IteratorStart.GT, makeKeyFirst(holder.key())), holder);
    }

    /**
     * Put a new entry into the map of a given holder.
     * @param holder the holder of the map
     * @param key the key to put
     * @param ref the key of the object reference to which to record
     * @param <K> the type of the key
     */
    public <K extends JMapKey> void put(JMapHolder<K> holder, K key, JObjectKey ref) {
        curTx.put(new JMapEntry<>(holder.key(), key, ref));
    }

    /**
     * Get an entry from the map of a given holder.
     * @param holder the holder of the map
     * @param key the key to get
     * @return an optional containing the entry if it exists, or an empty optional if it does not
     * @param <K> the type of the key
     */
    public <K extends JMapKey> Optional<JMapEntry<K>> get(JMapHolder<K> holder, K key) {
        return curTx.get(JMapEntry.class, makeKey(holder.key(), key)).map(e -> (JMapEntry<K>) e);
    }

    /**
     * Delete an entry from the map of a given holder.
     * @param holder the holder of the map
     * @param key the key to delete
     * @param <K> the type of the key
     */
    public <K extends JMapKey> void delete(JMapHolder<K> holder, K key) {
        curTx.delete(makeKey(holder.key(), key));
    }

    /**
     * Delete all entries from the map of a given holder.
     * @param holder the holder of the map
     * @param <K> the type of the key
     */
    public <K extends JMapKey> void deleteAll(JMapHolder<K> holder) {
        ArrayList<K> collectedKeys = new ArrayList<>();
        try (var it = getIterator(holder)) {
            while (it.hasNext()) {
                var curKey = it.peekNextKey();
                collectedKeys.add(curKey);
                it.skip();
            }
        }

        for (var curKey : collectedKeys) {
            delete(holder, curKey);
            Log.tracev("Removed map entry {0} to {1}", holder.key(), curKey);
        }
    }

}
