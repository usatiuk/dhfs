package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JObjectKey;

import java.io.Serializable;
import java.util.Collection;

public interface Op extends Serializable {
    Collection<JObjectKey> getEscapedRefs();
}
