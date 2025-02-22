package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

public interface CloseableKvIterator<K extends Comparable<K>, V> extends Iterator<Pair<K, V>>, AutoCloseableNoThrow {
    K peekNextKey();
}
