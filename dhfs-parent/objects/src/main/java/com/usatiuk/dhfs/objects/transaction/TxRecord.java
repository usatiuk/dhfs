package com.usatiuk.dhfs.objects.transaction;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public class TxRecord {
    public interface TxObjectRecord<T> {
        JObjectKey key();
    }

    public record TxObjectRecordWrite<T extends JData>(JData data) implements TxObjectRecord<T> {
        @Override
        public JObjectKey key() {
            return data.key();
        }
    }

    public record TxObjectRecordDeleted(JObjectKey key) implements TxObjectRecord<JData> {
    }
}
