package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

public class TxRecord {
    public interface TxObjectRecord<T> {
        JObjectKey key();
    }

    public interface TxObjectRecordWrite<T> extends TxObjectRecord<T> {
        JData data();
    }

    public record TxObjectRecordWriteChecked<T extends JData>(JData data) implements TxObjectRecordWrite<T> {
        @Override
        public JObjectKey key() {
            return data.key();
        }
    }

    public record TxObjectRecordNewWrite<T extends JData>(JData data) implements TxObjectRecordWrite<T> {
        @Override
        public JObjectKey key() {
            return data.key();
        }
    }

    public record TxObjectRecordDeleted(JObjectKey key) implements TxObjectRecord<JData> {
    }

}
