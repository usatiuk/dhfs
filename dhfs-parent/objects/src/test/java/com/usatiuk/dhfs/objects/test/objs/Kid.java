package com.usatiuk.dhfs.objects.test.objs;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObject;
import com.usatiuk.dhfs.objects.JObjectInterface;

public class Kid extends JObject {

    public Kid(JObjectInterface jObjectInterface, KidData data) {
        super(jObjectInterface, data);
    }

    @Override
    public JData getData() {
        return _data;
    }

}
