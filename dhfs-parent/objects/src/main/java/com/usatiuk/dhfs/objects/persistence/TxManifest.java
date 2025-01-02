package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.JObjectKey;

import java.io.Serializable;
import java.util.Collection;

// FIXME: Serializable
public record TxManifest(Collection<JObjectKey> written, Collection<JObjectKey> deleted) implements Serializable {
}
