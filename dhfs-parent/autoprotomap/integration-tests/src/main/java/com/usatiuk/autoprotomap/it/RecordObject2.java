package com.usatiuk.autoprotomap.it;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;

@ProtoMirror(RecordObject2Proto.class)
public record RecordObject2(String key, int value) implements InterfaceObject {
}
