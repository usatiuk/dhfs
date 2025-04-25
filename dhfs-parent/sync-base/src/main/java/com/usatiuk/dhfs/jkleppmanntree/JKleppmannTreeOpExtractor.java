package com.usatiuk.dhfs.jkleppmanntree;

import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.RemoteTransaction;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.repository.invalidation.InvalidationQueueService;
import com.usatiuk.dhfs.repository.invalidation.Op;
import com.usatiuk.dhfs.repository.invalidation.OpExtractor;
import com.usatiuk.dhfs.repository.syncmap.DtoMapperService;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

@ApplicationScoped
public class JKleppmannTreeOpExtractor implements OpExtractor<JKleppmannTreePersistentData> {
    @Inject
    TransactionManager txm;
    @Inject
    Transaction curTx;
    @Inject
    RemoteTransaction remoteTransaction;
    @Inject
    DtoMapperService dtoMapperService;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    InvalidationQueueService invalidationQueueService;


    @Override
    public Pair<List<Op>, Runnable> extractOps(JKleppmannTreePersistentData data, PeerId peerId) {
        return txm.run(() -> {
            var tree = jKleppmannTreeManager.getTree(data.key());

            if (!tree.hasPendingOpsForHost(peerId))
                return Pair.of(List.of(tree.getPeriodicPushOp()), (Runnable) () -> {
                });

            var ops = tree.getPendingOpsForHost(peerId, 100);

            if (tree.hasPendingOpsForHost(peerId)) {
                curTx.onCommit(() -> invalidationQueueService.pushInvalidationToOneNoDelay(peerId, data.key()));
            }
            var key = data.key();
            return Pair.<List<Op>, Runnable>of(ops, (Runnable) () -> {
                txm.run(() -> {
                    var commitTree = jKleppmannTreeManager.getTree(key);
                    for (var op : ops) {
                        commitTree.commitOpForHost(peerId, op);
                    }
                });
            });
        });
    }
}
