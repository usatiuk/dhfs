package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.invalidation.OpHandler;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class JKleppmannTreeOpHandler implements OpHandler<JKleppmannTreeOpWrapper> {
@Inject
    TransactionManager txm;
    @Inject
    Transaction curTx;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    InvalidationQueueService invalidationQueueService;

    @Override
    public void handleOp(PeerId from, JKleppmannTreeOpWrapper op) {
        txm.run(()->{
            var tree = jKleppmannTreeManager.getTree(op.treeName()).orElseThrow();
            tree.acceptExternalOp(from, op);
            // Push ack op
            curTx.onCommit(() -> invalidationQueueService.pushInvalidationToOne(from, op.treeName()));
        });
    }
}
