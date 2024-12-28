package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.objects.common.runtime.JData;

import java.io.Serializable;

public interface ChunkData extends JData, Serializable {
    ByteString getData();

    void setData(ByteString data);
}
