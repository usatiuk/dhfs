package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.RemoteObjectMeta;
import com.usatiuk.dhfs.objects.RemoteTransaction;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.objects.repository.JDataRemoteDto;
import com.usatiuk.dhfs.objects.repository.JDataRemotePush;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.syncmap.DtoMapperService;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

@ApplicationScoped
public class OpPusher {
    @Inject
    Transaction curTx;
    @Inject
    TransactionManager txm;
    @Inject
    RemoteTransaction remoteTransaction;
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    InvalidationQueueService invalidationQueueService;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    DtoMapperService dtoMapperService;

    public void doPush(InvalidationQueueEntry entry) {
        List<Op> info = txm.run(() -> {
            var obj = curTx.get(JData.class, entry.key()).orElse(null);
            switch (obj) {
                case RemoteObjectMeta remote -> {
                    JDataRemoteDto data =
                            remote.knownType().isAnnotationPresent(JDataRemotePush.class)
                                    ? remoteTransaction.getData(remote.knownType(), entry.key())
                                    .map(d -> dtoMapperService.toDto(d, d.dtoClass())).orElse(null)
                                    : null;

                    if (remote.knownType().isAnnotationPresent(JDataRemotePush.class) && data == null) {
                        Log.warnv("Failed to get data for push {0} of type {1}", entry.key(), remote.knownType());
                    }
                    return List.of(new IndexUpdateOp(entry.key(), remote.changelog(), data));
                }
                case JKleppmannTreePersistentData pd -> {
                    var tree = jKleppmannTreeManager.getTree(pd.key());

                    if (!tree.hasPendingOpsForHost(entry.peer()))
                        return null;

                    var ops = tree.getPendingOpsForHost(entry.peer(), 1);

                    if (tree.hasPendingOpsForHost(entry.peer()))
                        invalidationQueueService.pushInvalidationToOne(entry.peer(), pd.key());

                    return ops;
                }
                case null,
                     default -> {
                    return null;
                }
            }
        });
        if (info == null) {
            return;
        }
        remoteObjectServiceClient.pushOps(entry.peer(), info);
        txm.run(() -> {
            var obj = curTx.get(JData.class, entry.key()).orElse(null);
            switch (obj) {
                case JKleppmannTreePersistentData pd: {
                    var tree = jKleppmannTreeManager.getTree(pd.key());
                    for (var op : info) {
                        tree.commitOpForHost(entry.peer(), op);
                    }
                    break;
                }
                case null:
                default:
            }
        });

    }
}
