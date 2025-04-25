package com.usatiuk.dhfs.invalidation;

import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.remoteobj.RemoteObjectMeta;
import com.usatiuk.dhfs.remoteobj.RemoteTransaction;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.dhfs.remoteobj.JDataRemotePush;
import com.usatiuk.dhfs.syncmap.DtoMapperService;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
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
    @Inject
    DtoMapperService dtoMapperService;

    @Override
    public Pair<List<Op>, Runnable> extractOps(RemoteObjectMeta data, PeerId peerId) {
        return txm.run(() -> {
            JDataRemoteDto dto =
                    data.knownType().isAnnotationPresent(JDataRemotePush.class)
                            ? remoteTransaction.getData(data.knownType(), data.key())
                            .map(d -> dtoMapperService.toDto(d, d.dtoClass())).orElse(null)
                            : null;

            if (data.knownType().isAnnotationPresent(JDataRemotePush.class) && dto == null) {
                Log.warnv("Failed to get data for push {0} of type {1}", data.key(), data.knownType());
            }
            return Pair.of(List.of(new IndexUpdateOp(data.key(), data.changelog(), dto)), () -> {
            });
        });
    }
}
