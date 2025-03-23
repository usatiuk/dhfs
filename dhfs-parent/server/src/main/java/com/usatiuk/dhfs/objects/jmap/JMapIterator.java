package com.usatiuk.dhfs.objects.jmap;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.iterators.CloseableKvIterator;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

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
        if (!_backing.peekNextKey().name().startsWith(_prefix.name())) {
            _backing.skip();
            if (!_backing.peekNextKey().name().startsWith(_prefix.name())) {
                _hasNext = false;
            }
        }
    }

    public K keyToKey(JObjectKey key) {
        var keyPart = key.name().substring(_prefix.name().length());
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
        assert next.getKey().name().startsWith(_prefix.name());
        advance();
        return Pair.of(keyToKey(next.getKey()), (JMapEntry<K>) next.getValue());
    }
}
