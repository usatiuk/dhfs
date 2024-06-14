package com.usatiuk.dhfs.storage.objects.jrepository;

import io.smallrye.mutiny.Uni;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface JObjectRepository {
    @Nonnull
    Uni<Optional<JObject>> readJObject(String namespace, String name);
    @Nonnull
    <T extends JObject> Uni<Optional<T>> readJObjectChecked(String namespace, String name, Class<T> clazz);
    @Nonnull
    Uni<Void> writeJObject(String namespace, JObject object);
}
