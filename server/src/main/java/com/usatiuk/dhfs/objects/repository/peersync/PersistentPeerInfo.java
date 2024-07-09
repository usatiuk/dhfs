package com.usatiuk.dhfs.objects.repository.peersync;

import com.usatiuk.dhfs.files.conflicts.NotImplementedConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.PushResolution;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serial;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Getter
@AllArgsConstructor
@PushResolution
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

    public boolean assumeUnique() {
        return true;
    }

    // FIXME: Maybe check the certs?
    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return NotImplementedConflictResolver.class;
    }

    @Override
    public Collection<String> extractRefs() {
        return List.of();
    }
}
