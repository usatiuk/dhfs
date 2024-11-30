package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.dhfs.objects.JObjectKey;

import java.io.Serializable;
import java.util.List;

// FIXME: Serializable
public interface TxManifest extends Serializable {
    List<JObjectKey> getWritten();

    List<JObjectKey> getDeleted();
}
