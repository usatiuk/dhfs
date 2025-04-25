package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.remoteobj.RemoteObjectMeta;
import com.usatiuk.dhfs.remoteobj.RemoteTransaction;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

@ApplicationScoped
public class RemoteObjectMetaOpExtractor implements OpExtractor<RemoteObjectMeta> {
    @Inject
    TransactionManager txm;
    @Inject
    Transaction curTx;
    @Inject
    RemoteTransaction remoteTransaction;

    @Override
    public Pair<List<Op>, Runnable> extractOps(RemoteObjectMeta data, PeerId peerId) {
        return txm.run(() -> {
            return Pair.of(List.of(new IndexUpdateOp(data.key(), data.changelog())), () -> {
            });
        });
    }
}
