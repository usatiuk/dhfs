package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeOpWrapper;
import com.usatiuk.dhfs.objects.transaction.Transaction;
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

    public void handleOp(PeerId from, Op op) {
        if (op instanceof IndexUpdateOp iu) {
            pushOpHandler.handlePush(from, iu);
        } else if (op instanceof JKleppmannTreeOpWrapper jk) {
            var tree = jKleppmannTreeManager.getTree(jk.treeName());
            tree.acceptExternalOp(from, jk);
        }
    }
}
