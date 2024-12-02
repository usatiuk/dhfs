package com.usatiuk.dhfs.objects.data;

import com.usatiuk.objects.common.JData;
import com.usatiuk.objects.common.JObjectKey;

public interface Parent extends JData {
    JObjectKey getName();

    String getLastName();

    void setLastName(String lastName);

    JObjectKey getKidKey();

    void setKidKey(JObjectKey kid);
}
