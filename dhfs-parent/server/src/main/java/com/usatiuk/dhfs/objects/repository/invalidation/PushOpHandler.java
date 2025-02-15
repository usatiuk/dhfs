package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.RemoteTransaction;
import com.usatiuk.dhfs.objects.repository.SyncHandler;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class PushOpHandler {
    @Inject
    Transaction curTx;
    @Inject
    SyncHandler syncHandler;
    @Inject
    RemoteTransaction remoteTransaction;

    public void handlePush(PeerId peer, IndexUpdateOp obj) {
        syncHandler.handleRemoteUpdate(peer, obj.key(), obj.changelog(), null);
    }
}
