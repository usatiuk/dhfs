package com.usatiuk.autoprotomap.it;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import jakarta.inject.Singleton;

@Singleton
public class CustomObjectSerializer implements ProtoSerializer<CustomObjectProto, CustomObject> {
    @Override
    public CustomObject deserialize(CustomObjectProto message) {
        return new CustomObject(2);
    }

    @Override
    public CustomObjectProto serialize(CustomObject object) {
        return CustomObjectProto.newBuilder().setTest(1).build();
    }
}
