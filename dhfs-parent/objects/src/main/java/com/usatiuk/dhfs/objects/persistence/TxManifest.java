package com.usatiuk.dhfs.objects.persistence;

import com.usatiuk.objects.common.JObjectKey;

import java.io.Serializable;
import java.util.List;

// FIXME: Serializable
public interface TxManifest extends Serializable {
    List<JObjectKey> getWritten();

    List<JObjectKey> getDeleted();
}
