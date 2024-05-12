package com.usatiuk.dhfs.storage.data;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;

@Accessors(chain = true)
@Getter
@Setter
public class Object {
    Namespace namespace;

    String name;
    ByteBuffer data;
}
