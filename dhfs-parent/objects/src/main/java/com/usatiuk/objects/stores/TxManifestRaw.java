package com.usatiuk.objects.stores;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.JObjectKey;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.Collection;

public record TxManifestRaw(Collection<Pair<JObjectKey, ByteString>> written,
                            Collection<JObjectKey> deleted) implements Serializable {
}
