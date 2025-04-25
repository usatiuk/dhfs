package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.jkleppmanntree.JKleppmannTreePeriodicPushOp;
import com.usatiuk.objects.transaction.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class OpHandler {
    @Inject
    PushOpHandler pushOpHandler;
    @Inject
    Transaction curTx;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    InvalidationQueueService invalidationQueueService;

    public void handleOp(PeerId from, Op op) {
        if (op instanceof IndexUpdateOp iu) {
            pushOpHandler.handlePush(from, iu);
        } else if (op instanceof JKleppmannTreeOpWrapper jk) {
            var tree = jKleppmannTreeManager.getTree(jk.treeName());
            tree.acceptExternalOp(from, jk);
            curTx.onCommit(() -> invalidationQueueService.pushInvalidationToOne(from, jk.treeName()));
        } else if (op instanceof JKleppmannTreePeriodicPushOp pop) {
            var tree = jKleppmannTreeManager.getTree(pop.treeName());
            tree.acceptExternalOp(from, pop);
        }
    }
}
