package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.JObjectKey;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.Collection;

// FIXME: Serializable
public record TxManifestObj<T>(Collection<Pair<JObjectKey, T>> written,
                               Collection<JObjectKey> deleted) implements Serializable {
}
