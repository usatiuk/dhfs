package com.usatiuk.dhfs.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JObjectKey;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.Collection;

// FIXME: Serializable
public record TxManifestRaw(Collection<Pair<JObjectKey, ByteString>> written,
                            Collection<JObjectKey> deleted) implements Serializable {
}
