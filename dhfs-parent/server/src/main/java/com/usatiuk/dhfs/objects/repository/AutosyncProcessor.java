package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.iterators.IteratorStart;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.objects.transaction.TransactionManager;
import com.usatiuk.dhfs.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class AutosyncProcessor {
    private final HashSetDelayedBlockingQueue<JObjectKey> _pending = new HashSetDelayedBlockingQueue<>(0);
    private final HashSetDelayedBlockingQueue<JObjectKey> _retries = new HashSetDelayedBlockingQueue<>(10000); //FIXME:
    @Inject
    TransactionManager txm;
    @ConfigProperty(name = "dhfs.objects.autosync.threads")
    int autosyncThreads;
    @Inject
    ExecutorService executorService;
    @Inject
    Transaction curTx;
    @Inject
    RemoteTransaction remoteTx;
    @ConfigProperty(name = "dhfs.objects.autosync.download-all")
    boolean downloadAll;

    private ExecutorService _autosyncExcecutor;

    void init(@Observes @Priority(300) StartupEvent event) {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("autosync-%d")
                .build();

        _autosyncExcecutor = Executors.newFixedThreadPool(autosyncThreads, factory);
        for (int i = 0; i < autosyncThreads; i++) {
            _autosyncExcecutor.submit(this::autosync);
        }

        if (downloadAll)
            executorService.submit(() -> {
                Log.info("Adding all to autosync");
                List<JObjectKey> objs = new LinkedList<>();
                txm.run(() -> {
                    try (var it = curTx.getIterator(IteratorStart.GE, JObjectKey.first())) {
                        while (it.hasNext()) {
                            var key = it.peekNextKey();
                            objs.add(key);
                            // TODO: Nested transactions
                            it.skip();
                        }
                    }
                });

                for (var obj : objs) {
                    txm.run(() -> {
                        var gotObj = curTx.get(JData.class, obj).orElse(null);
                        if (!(gotObj instanceof RemoteObjectMeta meta))
                            return;
                        if (!meta.hasLocalData())
                            add(meta.key());
                    });
                }
                Log.info("Adding all to autosync: finished");
            });
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _autosyncExcecutor.shutdownNow();
    }

    public void add(JObjectKey name) {
        _pending.add(name);
    }

    private void autosync() {
        try {
            while (!Thread.interrupted()) {
                JObjectKey name = null;

                while (name == null) {
                    name = _pending.tryGet();
                    if (name == null)
                        name = _retries.tryGet();
                    if (name == null)
                        name = _pending.get(1000L); //FIXME:
                }

                try {
                    JObjectKey finalName = name;
                    boolean ok = txm.run(() -> {
                        var obj = remoteTx.getMeta(finalName).orElse(null);
                        if (obj == null) {
                            Log.debugv("Not downloading object {0}, not found", finalName);
                            return true;
                        }
                        if (obj.hasLocalData()) {
                            Log.debugv("Not downloading object {0}, already have local data", finalName);
                            return true;
                        }
                        var data = remoteTx.getData(JDataRemote.class, finalName);
                        return data.isPresent();
                    });
                    if (ok) {
                        Log.debugv("Downloaded object {0}", name);
                    }
                    if (!ok) {
                        Log.debug("Failed downloading object " + name + ", will retry.");
                        _retries.add(name);
                    }
                } catch (Exception e) {
                    Log.debug("Failed downloading object " + name + ", will retry.", e);
                    _retries.add(name);
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Autosync thread exiting");

    }
}
