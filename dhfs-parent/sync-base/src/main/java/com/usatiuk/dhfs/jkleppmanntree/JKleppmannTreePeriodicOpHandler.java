package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.invalidation.OpHandler;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.transaction.TransactionManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class JKleppmannTreePeriodicOpHandler implements OpHandler<JKleppmannTreePeriodicPushOp> {
    @Inject
    TransactionManager txm;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;

    @Override
    public void handleOp(PeerId from, JKleppmannTreePeriodicPushOp op) {
        txm.run(() -> {
            var tree = jKleppmannTreeManager.getTree(op.treeName()).orElseThrow();
            tree.acceptExternalOp(from, op);
        });
    }
}
