package com.usatiuk.dhfs.repository.invalidation;

import com.usatiuk.objects.JData;
import com.usatiuk.dhfs.RemoteObjectMeta;
import com.usatiuk.dhfs.RemoteTransaction;
import com.usatiuk.dhfs.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreePersistentData;
import com.usatiuk.dhfs.repository.JDataRemoteDto;
import com.usatiuk.dhfs.repository.JDataRemotePush;
import com.usatiuk.dhfs.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.repository.syncmap.DtoMapperService;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public Pair<List<Op>, List<Runnable>> preparePush(InvalidationQueueEntry entry) {
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
                        return List.of(tree.getPeriodicPushOp());

                    var ops = tree.getPendingOpsForHost(entry.peer(), 100);

                    if (tree.hasPendingOpsForHost(entry.peer())) {
                        invalidationQueueService.pushInvalidationToOneNoDelay(entry.peer(), pd.key());
                    }
                    return ops;
                }
                case null,
                     default -> {
                    return List.of();
                }
            }
        });
        List<Runnable> commits = info.stream().<Runnable>map(o -> {
            return () -> {
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
            };
        }).toList();

        return Pair.of(info, commits);
    }
}
