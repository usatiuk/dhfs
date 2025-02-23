package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.tuple.Pair;

import java.util.NoSuchElementException;
import java.util.function.Function;

// Also checks that the next provided item is always consistent after a refresh
public class InconsistentKvIteratorWrapper<K extends Comparable<K>, V> implements CloseableKvIterator<K, V> {
    private CloseableKvIterator<K, V> _backing;
    private final Function<Pair<IteratorStart, K>, CloseableKvIterator<K, V>> _iteratorSupplier;
    private K _lastReturnedKey = null;
    private K _peekedKey = null;
    private boolean _peekedNext = false;
    private final Pair<IteratorStart, K> _initialStart;

    public InconsistentKvIteratorWrapper(Function<Pair<IteratorStart, K>, CloseableKvIterator<K, V>> iteratorSupplier, IteratorStart start, K key) {
        _iteratorSupplier = iteratorSupplier;
        _initialStart = Pair.of(start, key);
        while (true) {
            try {
                _backing = _iteratorSupplier.apply(Pair.of(start, key));
                break;
            } catch (StaleIteratorException ignored) {
                continue;
            }
        }
    }

    private void refresh() {
        Log.tracev("Refreshing iterator: {0}", _backing);
        _backing.close();
        if (_peekedKey != null) {
            _backing = _iteratorSupplier.apply(Pair.of(IteratorStart.GE, _peekedKey));
            if (!_backing.hasNext() || !_backing.peekNextKey().equals(_peekedKey)) {
                assert false;
            }
        } else if (_lastReturnedKey != null) {
            _backing = _iteratorSupplier.apply(Pair.of(IteratorStart.GT, _lastReturnedKey));
        } else {
            _backing = _iteratorSupplier.apply(_initialStart);
        }

        if (_peekedNext && !_backing.hasNext()) {
            assert false;
        }
    }

    @Override
    public K peekNextKey() {
        while (true) {
            if (_peekedKey != null) {
                return _peekedKey;
            }
            try {
                _peekedKey = _backing.peekNextKey();
                assert _lastReturnedKey == null || _peekedKey.compareTo(_lastReturnedKey) > 0;
            } catch (NoSuchElementException ignored) {
                assert !_peekedNext;
                throw ignored;
            } catch (StaleIteratorException ignored) {
                refresh();
                continue;
            }
            _peekedNext = true;
            Log.tracev("Peeked key: {0}", _peekedKey);
            return _peekedKey;
        }
    }

    @Override
    public void skip() {
        while (true) {
            try {
                _lastReturnedKey = _backing.peekNextKey();
                _backing.skip();
                _peekedNext = false;
                _peekedKey = null;
                return;
            } catch (NoSuchElementException ignored) {
                assert !_peekedNext;
                throw ignored;
            } catch (StaleIteratorException ignored) {
                refresh();
                continue;
            }
        }
    }

    @Override
    public void close() {
        _backing.close();
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (_peekedNext) {
                return true;
            }
            try {
                _peekedNext = _backing.hasNext();
                Log.tracev("Peeked next: {0}", _peekedNext);
                return _peekedNext;
            } catch (StaleIteratorException ignored) {
                refresh();
                continue;
            }
        }
    }

    @Override
    public Pair<K, V> next() {
        while (true) {
            try {
                var got = _backing.next();
                assert _lastReturnedKey == null || _peekedKey.compareTo(_lastReturnedKey) > 0;
                _peekedNext = false;
                _peekedKey = null;
                _lastReturnedKey = got.getKey();
                return got;
            } catch (NoSuchElementException ignored) {
                assert !_peekedNext;
                throw ignored;
            } catch (StaleIteratorException ignored) {
                refresh();
                continue;
            }
        }
    }

}
