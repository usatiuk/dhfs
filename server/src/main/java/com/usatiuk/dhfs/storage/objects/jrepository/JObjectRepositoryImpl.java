package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.data.Namespace;
import com.usatiuk.dhfs.storage.objects.data.Object;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.SerializationUtils;

import javax.annotation.Nonnull;
import java.util.Optional;

@ApplicationScoped
public class JObjectRepositoryImpl implements JObjectRepository {
    @Inject
    ObjectRepository objectRepository;

    @Nonnull
    @Override
    public Optional<JObject> readJObject(String namespace, String name) {
        var read = objectRepository.readObject(namespace, name);
        java.lang.Object obj = DeserializationHelper.deserialize(read.getData());
        if (!(obj instanceof JObject)) {
            Log.error("Read object is not a JObject: " + namespace + "/" + name);
            return Optional.empty();
        }
        return Optional.of((JObject) obj);
    }

    @Nonnull
    @Override
    public <T extends JObject> Optional<T> readJObjectChecked(String namespace, String name, Class<T> clazz) {
        var read = readJObject(namespace, name);
        if (read.isEmpty()) return Optional.empty();

        if (!clazz.isAssignableFrom(read.get().getClass())) {
            Log.error("Read object type mismatch: " + namespace + "/" + name);
            return Optional.empty();
        }
        return Optional.of((T) read.get());
    }

    @Nonnull
    @Override
    public void writeJObject(String namespace, JObject object) {
        final var obj = new Object(
                new Namespace(namespace),
                object.getName(),
                SerializationUtils.serialize(object));
        objectRepository.writeObject(namespace, obj, object.assumeUnique());
    }
}
