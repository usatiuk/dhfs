package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.objects.common.runtime.JObjectKey;

import java.io.Serializable;
import java.util.Collection;

// FIXME: Serializable
public record TxManifest(Collection<JObjectKey> written, Collection<JObjectKey> deleted) implements Serializable {
}
