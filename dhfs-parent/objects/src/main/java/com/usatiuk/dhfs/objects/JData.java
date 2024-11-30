package com.usatiuk.dhfs.objects;

import java.util.function.Function;

public interface JData {
    JObjectKey getKey();

    JData bindCopy();

    Function<JObjectInterface, JObject> binder();
}
