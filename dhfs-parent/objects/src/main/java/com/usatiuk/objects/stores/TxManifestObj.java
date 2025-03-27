package com.usatiuk.objects.stores;

import com.usatiuk.objects.JObjectKey;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.Collection;

// FIXME: Serializable
public record TxManifestObj<T>(Collection<Pair<JObjectKey, T>> written,
                               Collection<JObjectKey> deleted) implements Serializable {
}
