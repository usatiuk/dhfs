package com.usatiuk.objects;

import com.usatiuk.objects.data.Parent;
import com.usatiuk.objects.transaction.PreCommitTxHook;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectSpy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(TempDataProfile.class)
public class PreCommitTxHookTest {
    @Inject
    TransactionManager txm;

    @Inject
    Transaction curTx;
    @InjectSpy
    private DummyPreCommitTxHook spyHook;

    @Test
    void createObject() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentCreate2"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("ParentCreate2")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });

        ArgumentCaptor<JData> dataCaptor = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JObjectKey> keyCaptor = ArgumentCaptor.forClass(JObjectKeyImpl.class);
        Mockito.verify(spyHook, Mockito.times(1)).onCreate(keyCaptor.capture(), dataCaptor.capture());
        Assertions.assertEquals("John", ((Parent) dataCaptor.getValue()).name());
        Assertions.assertEquals(new JObjectKeyImpl("ParentCreate2"), keyCaptor.getValue());
    }

    @Test
    void deleteObject() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentDel"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("ParentDel")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });

        txm.run(() -> {
            curTx.delete(new JObjectKeyImpl("ParentDel"));
        });

        ArgumentCaptor<JData> dataCaptor = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JObjectKey> keyCaptor = ArgumentCaptor.forClass(JObjectKeyImpl.class);
        Mockito.verify(spyHook, Mockito.times(1)).onDelete(keyCaptor.capture(), dataCaptor.capture());
        Assertions.assertEquals("John", ((Parent) dataCaptor.getValue()).name());
        Assertions.assertEquals(new JObjectKeyImpl("ParentDel"), keyCaptor.getValue());
    }

    @Test
    void editObject() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentEdit"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentEdit"), "John changed");
            curTx.put(newParent);
        });

        ArgumentCaptor<JData> dataCaptorOld = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JData> dataCaptorNew = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JObjectKey> keyCaptor = ArgumentCaptor.forClass(JObjectKeyImpl.class);
        Mockito.verify(spyHook, Mockito.times(1)).onChange(keyCaptor.capture(), dataCaptorOld.capture(), dataCaptorNew.capture());
        Assertions.assertEquals("John", ((Parent) dataCaptorOld.getValue()).name());
        Assertions.assertEquals("John changed", ((Parent) dataCaptorNew.getValue()).name());
        Assertions.assertEquals(new JObjectKeyImpl("ParentEdit"), keyCaptor.getValue());
    }

    @Test
    void editObjectWithGet() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentEdit2"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("ParentEdit2")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            curTx.put(parent.withName("John changed"));
        });

        ArgumentCaptor<JData> dataCaptorOld = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JData> dataCaptorNew = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JObjectKey> keyCaptor = ArgumentCaptor.forClass(JObjectKeyImpl.class);
        Mockito.verify(spyHook, Mockito.times(1)).onChange(keyCaptor.capture(), dataCaptorOld.capture(), dataCaptorNew.capture());
        Assertions.assertEquals("John", ((Parent) dataCaptorOld.getValue()).name());
        Assertions.assertEquals("John changed", ((Parent) dataCaptorNew.getValue()).name());
        Assertions.assertEquals(new JObjectKeyImpl("ParentEdit2"), keyCaptor.getValue());
    }

    @ApplicationScoped
    public static class DummyPreCommitTxHook implements PreCommitTxHook {
    }

}
