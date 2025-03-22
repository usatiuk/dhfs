package com.usatiuk.dhfs.objects.jkleppmanntree;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.repository.InitialSyncProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class JKleppmannTreeInitialSyncProcessor implements InitialSyncProcessor<JKleppmannTreePersistentData> {
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;

    @Override
    public void prepareForInitialSync(PeerId from, JObjectKey key) {
        var tree = jKleppmannTreeManager.getTree(key);
        tree.recordBootstrap(from);
    }
}
