package com.usatiuk.dhfs.objects.repository.persistence;

import java.io.Serializable;
import java.util.List;

// FIXME: Serializable
public interface TxManifest extends Serializable {
    List<String> getWritten();

    List<String> getDeleted();
}
