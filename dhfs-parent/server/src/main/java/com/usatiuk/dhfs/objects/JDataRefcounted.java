package com.usatiuk.dhfs.objects;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.util.Collection;
import java.util.List;

public interface JDataRefcounted extends JData {
    Collection<JObjectKey> getRefsFrom();

    void setRefsFrom(Collection<JObjectKey> refs);

    default Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }
}
