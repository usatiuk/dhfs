package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
public class ObjectMetadataSerializer implements ProtoSerializer<ObjectMetadataP, ObjectMetadata> {
    @Override
    public ObjectMetadataP serialize(ObjectMetadata object) {
        return ObjectMetadataP.newBuilder()
                .setName(object.getName())
                .putAllRemoteCopies(object.getRemoteCopies().entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)))
                .setKnownClass(object.getKnownClass().getName())
                .setSeen(object.isSeen())
                .setDeleted(object.isDeleted())
                .addAllConfirmedDeletes(() -> object.getConfirmedDeletes().stream().map(e -> e.toString()).iterator())
                .addAllReferrers(object.getReferrers())
                .putAllChangelog(object.getChangelog().entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)))
                .addAllSavedRefs(object.getSavedRefs() != null ? object.getSavedRefs() : Collections.emptyList())
                .setFrozen(object.isFrozen())
                .setHaveLocalCopy(object.isHaveLocalCopy())
                .build();
    }

    @Override
    public ObjectMetadata deserialize(ObjectMetadataP message) {
        try {
            var obj = new ObjectMetadata(message.getName(), true,
                    (Class<? extends JObjectData>) Class.forName(message.getKnownClass(), true, ObjectMetadata.class.getClassLoader()));
            if (!JObjectData.class.isAssignableFrom(obj.getKnownClass()))
                throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Class not inherited from JObjectData " + message.getKnownClass()));

            obj.getRemoteCopies().putAll(message.getRemoteCopiesMap().entrySet().stream().collect(Collectors.toMap(e -> UUID.fromString(e.getKey()), Map.Entry::getValue)));
            if (message.getSeen()) obj.markSeen();
            if (message.getDeleted()) obj.markDeleted();
            message.getConfirmedDeletesList().stream().map(UUID::fromString).forEach(o -> obj.getConfirmedDeletes().add(o));
            obj.getReferrersMutable().addAll(message.getReferrersList());
            obj.getChangelog().putAll(message.getChangelogMap().entrySet().stream().collect(Collectors.toMap(e -> UUID.fromString(e.getKey()), Map.Entry::getValue)));
            if (message.getSavedRefsCount() > 0)
                obj.setSavedRefs(new LinkedHashSet<>(message.getSavedRefsList()));
            if (message.getFrozen())
                obj.freeze();
            if (message.getHaveLocalCopy())
                obj.setHaveLocalCopy(true);

            return obj;
        } catch (ClassNotFoundException cx) {
            throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Could not find class " + message.getKnownClass()));
        }
    }
}
