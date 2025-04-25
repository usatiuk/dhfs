package com.usatiuk.dhfs.refcount;

import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;

public interface JDataRef extends Comparable<JDataRef>, Serializable {
    JObjectKey obj();
}
