package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.repository.InitialSyncProcessor;
import com.usatiuk.dhfs.repository.invalidation.InvalidationQueueService;
import com.usatiuk.objects.JObjectKey;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class JKleppmannTreeInitialSyncProcessor implements InitialSyncProcessor<JKleppmannTreePersistentData> {
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    InvalidationQueueService invalidationQueueService;

    @Override
    public void prepareForInitialSync(PeerId from, JObjectKey key) {
        var tree = jKleppmannTreeManager.getTree(key);
        tree.recordBootstrap(from);
    }

    @Override
    public void handleCrash(JObjectKey key) {
        invalidationQueueService.pushInvalidationToAll(key);
        Log.infov("Pushing after crash invalidation to all for {0}", key);
    }
}
