package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;

public interface TxBundle {
    long getId();

    void commit(String objName, ObjectMetadataP meta, JObjectDataP data);

    void commitMetaChange(String objName, ObjectMetadataP meta);

    void delete(String objName);
}
