package com.usatiuk.dhfs.objects.test.objs;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObject;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import lombok.experimental.Delegate;

public class Parent extends JObject {
    @Delegate
    private final ParentData _data;

    public Parent(Transaction Transaction, ParentData data) {
        super(Transaction);
        _data = data;
    }

    @Override
    public JData getData() {
        return _data;
    }

    public Kid getKid() {
        return _jObjectInterface.getObject(_data.getKidKey(), Kid.class);
    }
}
