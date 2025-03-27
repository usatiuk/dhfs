package com.usatiuk.dhfs;

import com.usatiuk.dhfs.objects.JObjectKey;

import java.io.Serializable;

public interface JDataRef extends Comparable<JDataRef>, Serializable {
    JObjectKey obj();
}
