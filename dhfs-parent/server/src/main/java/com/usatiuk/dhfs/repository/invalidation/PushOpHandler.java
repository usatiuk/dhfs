package com.usatiuk.dhfs.repository.invalidation;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.RemoteTransaction;
import com.usatiuk.dhfs.repository.SyncHandler;
import com.usatiuk.objects.transaction.Transaction;
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
        syncHandler.handleRemoteUpdate(peer, obj.key(), obj.changelog(), obj.data());
    }
}
