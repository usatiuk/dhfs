package com.usatiuk.dhfs.jmap;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Iterates over JMap entries of a given holder.
 * @param <K> the type of the key
 */
public class JMapIterator<K extends JMapKey> implements CloseableKvIterator<K, JMapEntry<K>> {
    private final CloseableKvIterator<JObjectKey, JData> _backing;
    private final JObjectKey _prefix;
    private boolean _hasNext = true;

    public JMapIterator(CloseableKvIterator<JObjectKey, JData> backing, JMapHolder<K> holder) {
        _backing = backing;
        _prefix = JMapHelper.makePrefix(holder.key());
        advance();
    }

    void advance() {
        assert _hasNext;
        if (!_backing.hasNext()) {
            _hasNext = false;
            return;
        }
        if (!_backing.peekNextKey().value().startsWith(_prefix.value())) {
            _backing.skip();
            if (!_backing.peekNextKey().value().startsWith(_prefix.value())) {
                _hasNext = false;
            }
        }
    }

    public K keyToKey(JObjectKey key) {
        var keyPart = key.value().substring(_prefix.value().length());
        return (K) JMapLongKey.of(Long.parseLong(keyPart));
    }

    @Override
    public K peekNextKey() {
        if (!_hasNext) {
            throw new IllegalStateException("No next element");
        }

        return keyToKey(_backing.peekNextKey());
    }

    @Override
    public void skip() {
        if (!_hasNext) {
            throw new IllegalStateException("No next element");
        }
        _backing.skip();
        advance();
    }

    @Override
    public K peekPrevKey() {
        throw new NotImplementedException();
    }

    @Override
    public Pair<K, JMapEntry<K>> prev() {
        throw new NotImplementedException();
    }

    @Override
    public boolean hasPrev() {
        throw new NotImplementedException();
    }

    @Override
    public void skipPrev() {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public boolean hasNext() {
        return _hasNext;
    }

    @Override
    public Pair<K, JMapEntry<K>> next() {
        if (!_hasNext) {
            throw new IllegalStateException("No next element");
        }
        var next = _backing.next();
        assert next.getKey().value().startsWith(_prefix.value());
        advance();
        return Pair.of(keyToKey(next.getKey()), (JMapEntry<K>) next.getValue());
    }
}
