package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.storage.objects.data.Namespace;
import com.usatiuk.dhfs.storage.objects.data.Object;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang3.SerializationUtils;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

@ApplicationScoped
public class JObjectRepositoryImpl implements JObjectRepository {
    @Inject
    ObjectRepository objectRepository;

    // Taken from SerializationUtils
    private static <T> T deserialize(final InputStream inputStream) {
        try (ClassLoaderObjectInputStream in = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), inputStream)) {
            final T obj = (T) in.readObject();
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> T deserialize(final byte[] objectData) {
        return deserialize(new ByteArrayInputStream(objectData));
    }

    @Nonnull
    @Override
    public Uni<Optional<JObject>> readJObject(String namespace, String name) {
        return objectRepository.readObject(namespace, name).map(o -> {
            java.lang.Object obj = deserialize(new ByteArrayInputStream(o.getData()));
            if (!(obj instanceof JObject)) {
                Log.error("Read object is not a JObject: " + namespace + "/" + name);
                return Optional.empty();
            }
            return Optional.of((JObject) obj);
        });
    }

    @Nonnull
    @Override
    public <T extends JObject> Uni<Optional<T>> readJObjectChecked(String namespace, String name, Class<T> clazz) {
        return readJObject(namespace, name).map(o -> {
            if (o.isEmpty()) return Optional.empty();

            if (!clazz.isAssignableFrom(o.get().getClass())) {
                Log.error("Read object type mismatch: " + namespace + "/" + name);
                return Optional.empty();
            }
            return Optional.of((T) o.get());
        });
    }

    @Nonnull
    @Override
    public Uni<Void> writeJObject(String namespace, JObject object) {
        final var obj = new Object(
                new Namespace(namespace),
                object.getName(),
                SerializationUtils.serialize(object));
        return objectRepository.writeObject(namespace, obj);
    }
}
