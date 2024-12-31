package com.usatiuk.objects.common.runtime;

import java.io.Serializable;

public record JObjectKey(String name) implements Serializable {
    public static JObjectKey of(String name) {
        return new JObjectKey(name);
    }
}
