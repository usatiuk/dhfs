package com.usatiuk.dhfs.storage.objects.repository.peersync;

import com.usatiuk.dhfs.storage.files.conflicts.NotImplementedConflictResolver;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.repository.ConflictResolver;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Getter
@AllArgsConstructor
public class PersistentPeerInfo extends JObjectData {
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
    public boolean pushResolution() {
        return true;
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
