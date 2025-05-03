package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.invalidation.IndexUpdateOp;
import com.usatiuk.dhfs.invalidation.OpHandler;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class IndexUpdateOpHandler implements OpHandler<IndexUpdateOp> {
    @Inject
    TransactionManager txm;
    @Inject
    Transaction curTx;
    @Inject
    SyncHandler syncHandler;

    @Override
    public void handleOp(PeerId from, IndexUpdateOp op) {
        txm.run(() -> {
            syncHandler.handleRemoteUpdate(from, op.key(), op.changelog(), op.data());
        });
    }
}
