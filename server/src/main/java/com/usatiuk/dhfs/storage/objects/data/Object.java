package com.usatiuk.dhfs.storage.objects.data;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
@Getter
@Setter
@AllArgsConstructor
public class Object {
    final Namespace namespace;

    final String name;
    final byte[] data;
}
