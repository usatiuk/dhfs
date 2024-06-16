package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdatePush;
import com.usatiuk.dhfs.objects.repository.distributed.IndexUpdateReply;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.persistence.ObjectPersistentStore;
import io.quarkus.logging.Log;
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
        var meta = objectIndexService.getOrCreateMeta(request.getName(), request.getAssumeUnique());
        meta.runWriteLocked((data) -> {
            if (meta.getMtime() == request.getMtime()) {
                data.getRemoteCopies().add(request.getSelfname());
                return null;
            }

            if (meta.getMtime() != request.getPrevMtime()) {
                if (!meta.getAssumeUnique()
                        || (meta.getAssumeUnique() != request.getAssumeUnique())) {
                    Log.error("Conflict!");
                    throw new NotImplementedException();
                }
            }

            meta.setMtime(request.getMtime());

            data.getRemoteCopies().clear();
            data.getRemoteCopies().add(request.getSelfname());

            try {
                objectPersistentStore.deleteObject(request.getName());
            } catch (Exception ignored) {
            }

            jObjectManager.invalidateJObject(data.getName());

            return null;
        });

        return IndexUpdateReply.newBuilder().build();
    }

}
