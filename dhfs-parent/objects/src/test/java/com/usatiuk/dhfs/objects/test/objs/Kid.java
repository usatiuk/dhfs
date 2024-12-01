package com.usatiuk.dhfs.objects.test.objs;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObject;
import com.usatiuk.dhfs.objects.transaction.Transaction;

public class Kid extends JObject {

    public Kid(Transaction Transaction, KidData data) {
        super(Transaction, data);
    }

    @Override
    public JData getData() {
        return _data;
    }

}
