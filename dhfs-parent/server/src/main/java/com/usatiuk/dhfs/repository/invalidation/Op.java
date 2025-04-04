package com.usatiuk.dhfs.repository.invalidation;

import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;
import java.util.Collection;

public interface Op extends Serializable {
    Collection<JObjectKey> getEscapedRefs();
}
