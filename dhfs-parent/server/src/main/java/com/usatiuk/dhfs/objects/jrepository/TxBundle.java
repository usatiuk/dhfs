package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;

public interface TxBundle {
    long getId();

    void commit(JObject<?> obj, ObjectMetadataP meta, JObjectDataP data);

    void commitMetaChange(JObject<?> obj, ObjectMetadataP meta);

    void delete(JObject<?> obj);
}
