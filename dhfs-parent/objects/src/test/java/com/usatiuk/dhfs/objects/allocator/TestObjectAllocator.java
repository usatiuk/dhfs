package com.usatiuk.dhfs.objects.allocator;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.ObjectAllocator;
import com.usatiuk.dhfs.objects.data.Kid;
import com.usatiuk.dhfs.objects.data.Parent;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TestObjectAllocator implements ObjectAllocator {
    @Override
    public <T extends JData> T create(Class<T> type, JObjectKey key) {
        if (type == Kid.class) {
            return type.cast(new KidDataNormal(key));
        } else if (type == Parent.class) {
            return type.cast(new ParentDataNormal(key));
        } else {
            throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    @Override
    public <T extends JData> ChangeTrackingJData<T> copy(T obj) {
        if (obj instanceof ChangeTrackerBase<?>) {
            throw new IllegalArgumentException("Cannot copy a ChangeTrackerBase object");
        }

        return switch (obj) {
            case KidDataNormal kid -> (ChangeTrackingJData<T>) new KidDataCT(kid);
            case ParentDataNormal parent -> (ChangeTrackingJData<T>) new ParentDataCT(parent);
            default -> throw new IllegalStateException("Unexpected value: " + obj);
        };
    }

    @Override
    public <T extends JData> T unmodifiable(T obj) {
        return obj; // TODO:
    }
}
