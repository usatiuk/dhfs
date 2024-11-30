package com.usatiuk.dhfs.objects;

import java.util.Optional;

public interface JObjectInterface {
    Optional<JObject> getObject(JObjectKey key);

    <T extends JObject> Optional<T> getObject(JObjectKey key, Class<T> type);
}
