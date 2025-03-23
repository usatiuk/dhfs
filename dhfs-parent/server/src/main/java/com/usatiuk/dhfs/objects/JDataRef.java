package com.usatiuk.dhfs.objects;

import java.io.Serializable;

public interface JDataRef extends Comparable<JDataRef>, Serializable {
    JObjectKey obj();
}
