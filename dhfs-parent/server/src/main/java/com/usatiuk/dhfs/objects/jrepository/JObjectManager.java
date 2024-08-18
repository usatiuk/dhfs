package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.utils.VoidFn;
import jakarta.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;

public interface JObjectManager {
    // FIXME:
    void runWriteListeners(JObject<?> obj, boolean metaChanged, boolean dataChanged);

    <T extends JObjectData> void registerWriteListener(Class<T> klass, WriteListenerFn fn);

    <T extends JObjectData> void registerMetaWriteListener(Class<T> klass, WriteListenerFn fn);

    Optional<JObject<?>> get(String name);

    Collection<String> findAll();

    // Put a new object
    <T extends JObjectData> JObject<T> put(T object, Optional<String> parent);

    <T extends JObjectData> JObject<T> putLocked(T object, Optional<String> parent);

    // Get an object with a name if it exists, otherwise create new one based on metadata
    // Should be used when working with objects referenced from the outside
    JObject<?> getOrPut(String name, Class<? extends JObjectData> klass, Optional<String> parent);

    JObject<?> getOrPutLocked(String name, Class<? extends JObjectData> klass, Optional<String> parent);

    enum ResolutionStrategy {
        NO_RESOLUTION,
        LOCAL_ONLY,
        REMOTE
    }

    @FunctionalInterface
    interface WriteListenerFn {
        void apply(JObject<?> obj);
    }

    @FunctionalInterface
    interface ObjectFnRead<T, R> {
        R apply(ObjectMetadata meta, @Nullable T data);
    }

    @FunctionalInterface
    interface ObjectFnWrite<T, R> {
        R apply(ObjectMetadata indexData, @Nullable T data, VoidFn bump, VoidFn invalidate);
    }

    @FunctionalInterface
    interface ObjectFnReadVoid<T> {
        void apply(ObjectMetadata meta, @Nullable T data);
    }

    @FunctionalInterface
    interface ObjectFnWriteVoid<T> {
        void apply(ObjectMetadata indexData, @Nullable T data, VoidFn bump, VoidFn invalidate);
    }

}
