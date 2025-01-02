package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.data.Parent;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectSpy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@QuarkusTest
public class PreCommitTxHookTest {
    @Inject
    TransactionManager txm;

    @Inject
    Transaction curTx;

    @ApplicationScoped
    public static class DummyPreCommitTxHook implements PreCommitTxHook {
    }

    @InjectSpy
    private DummyPreCommitTxHook spyHook;

    @Test
    void createObject() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("ParentCreate"), "John");
            curTx.put(newParent);
            curTx.put(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, new JObjectKey("ParentCreate")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            txm.commit();
        }

        ArgumentCaptor<JData> dataCaptor = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JObjectKey> keyCaptor = ArgumentCaptor.forClass(JObjectKey.class);
        Mockito.verify(spyHook, Mockito.times(1)).onCreate(keyCaptor.capture(), dataCaptor.capture());
        Assertions.assertEquals("John", ((Parent) dataCaptor.getValue()).name());
        Assertions.assertEquals(new JObjectKey("ParentCreate"), keyCaptor.getValue());
    }

    @Test
    void deleteObject() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("ParentDel"), "John");
            curTx.put(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, new JObjectKey("ParentDel")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            txm.commit();
        }

        {
            txm.begin();
            curTx.delete(new JObjectKey("ParentDel"));
            txm.commit();
        }

        ArgumentCaptor<JData> dataCaptor = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JObjectKey> keyCaptor = ArgumentCaptor.forClass(JObjectKey.class);
        Mockito.verify(spyHook, Mockito.times(1)).onDelete(keyCaptor.capture(), dataCaptor.capture());
        Assertions.assertEquals("John", ((Parent) dataCaptor.getValue()).name());
        Assertions.assertEquals(new JObjectKey("ParentDel"), keyCaptor.getValue());
    }

    @Test
    void editObject() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("ParentEdit"), "John");
            curTx.put(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("ParentEdit"), "John changed");
            curTx.put(newParent);
            txm.commit();
        }

        ArgumentCaptor<JData> dataCaptorOld = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JData> dataCaptorNew = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JObjectKey> keyCaptor = ArgumentCaptor.forClass(JObjectKey.class);
        Mockito.verify(spyHook, Mockito.times(1)).onChange(keyCaptor.capture(), dataCaptorOld.capture(), dataCaptorNew.capture());
        Assertions.assertEquals("John", ((Parent) dataCaptorOld.getValue()).name());
        Assertions.assertEquals("John changed", ((Parent) dataCaptorNew.getValue()).name());
        Assertions.assertEquals(new JObjectKey("ParentEdit"), keyCaptor.getValue());
    }

    @Test
    void editObjectWithGet() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("ParentEdit2"), "John");
            curTx.put(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, new JObjectKey("ParentEdit2")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            curTx.put(parent.toBuilder().name("John changed").build());
            txm.commit();
        }

        ArgumentCaptor<JData> dataCaptorOld = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JData> dataCaptorNew = ArgumentCaptor.forClass(JData.class);
        ArgumentCaptor<JObjectKey> keyCaptor = ArgumentCaptor.forClass(JObjectKey.class);
        Mockito.verify(spyHook, Mockito.times(1)).onChange(keyCaptor.capture(), dataCaptorOld.capture(), dataCaptorNew.capture());
        Assertions.assertEquals("John", ((Parent) dataCaptorOld.getValue()).name());
        Assertions.assertEquals("John changed", ((Parent) dataCaptorNew.getValue()).name());
        Assertions.assertEquals(new JObjectKey("ParentEdit2"), keyCaptor.getValue());
    }

}
