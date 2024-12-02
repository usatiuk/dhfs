package com.usatiuk.dhfs.objects.data;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public interface Parent extends JData {
    JObjectKey getName();

    String getLastName();
    void setLastName(String lastName);

    JObjectKey getKidKey();

    void setKidKey(JObjectKey kid);
}
