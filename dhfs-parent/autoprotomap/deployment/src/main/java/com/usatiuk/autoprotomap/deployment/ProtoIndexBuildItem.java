package com.usatiuk.autoprotomap.deployment;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import io.quarkus.builder.item.SimpleBuildItem;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.jboss.jandex.ClassInfo;

public final class ProtoIndexBuildItem extends SimpleBuildItem {
    BidiMap<ClassInfo, ClassInfo> protoMsgToObj = new DualHashBidiMap<>();
}
