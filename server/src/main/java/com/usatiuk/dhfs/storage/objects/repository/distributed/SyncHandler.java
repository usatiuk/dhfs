package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdatePush;
import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdateReply;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;

@ApplicationScoped
public class SyncHandler {
    @Inject
    ObjectPersistentStore objectPersistentStore;

    @Inject
    ObjectIndexService objectIndexService;

    @Inject
    JObjectManager jObjectManager;

    public IndexUpdateReply handleRemoteUpdate(IndexUpdatePush request) {
        var metaOpt = objectIndexService.getOrCreateMeta(request.getName(), request.getAssumeUnique());
        metaOpt.runWriteLocked(() -> {
            if (metaOpt.getMtime() == request.getMtime()) {
                metaOpt._remoteCopies.add(request.getSelfname());
                return null;
            }

            if (metaOpt.getMtime() != request.getPrevMtime()) {
                if (!metaOpt.getAssumeUnique()
                        || (metaOpt.getAssumeUnique() != request.getAssumeUnique())) {
                    Log.error("Conflict!");
                    throw new NotImplementedException();
                }
            }

            metaOpt.setMtime(request.getMtime());

            metaOpt._remoteCopies.clear();
            metaOpt._remoteCopies.add(request.getSelfname());

            try {
                objectPersistentStore.deleteObject(request.getName());
            } catch (Exception ignored) {
            }

            jObjectManager.invalidateJObject(metaOpt.getName());

            return null;
        });

        return IndexUpdateReply.newBuilder().build();
    }

}
