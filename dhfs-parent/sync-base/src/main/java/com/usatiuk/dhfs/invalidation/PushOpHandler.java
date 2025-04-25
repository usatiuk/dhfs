package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.remoteobj.RemoteTransaction;
import com.usatiuk.dhfs.remoteobj.SyncHandler;
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
        syncHandler.handleRemoteUpdate(peer, obj.key(), obj.changelog(), null);
    }
}
