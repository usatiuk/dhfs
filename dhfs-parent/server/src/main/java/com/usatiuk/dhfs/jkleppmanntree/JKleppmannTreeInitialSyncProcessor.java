package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.repository.InitialSyncProcessor;
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
