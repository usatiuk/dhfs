package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.files.conflicts.NoOpConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.AssumedUnique;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.PushResolution;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serial;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Getter
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@PushResolution
@AssumedUnique
public class PersistentPeerInfo extends JObjectData {
    @Serial
    private static final long serialVersionUID = 1;

    private final UUID _uuid;
    private final X509Certificate _certificate;

    public static String getNameFromUuid(UUID uuid) {
        return "peer_" + uuid;
    }

    @Override
    public String getName() {
        return getNameFromUuid(_uuid);
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return NoOpConflictResolver.class;
    }

    @Override
    public Collection<String> extractRefs() {
        return List.of();
    }
}
