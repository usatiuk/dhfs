package com.usatiuk.dhfs.peersync.structs;

import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.objects.JObjectKey;

import java.util.Collection;
import java.util.List;

//@ProtoMirror(JKleppmannTreeNodeMetaFileP.class)
public record JKleppmannTreeNodeMetaPeer(String name, JObjectKey peerId) implements JKleppmannTreeNodeMeta {
    public JKleppmannTreeNodeMetaPeer(PeerId id) {
        this(peerIdToNodeId(id).value(), id.id());
    }

    public static JObjectKey peerIdToNodeId(PeerId id) {
        return JObjectKey.of(id.toJObjectKey().value() + "_tree_node");
    }

    public static PeerId nodeIdToPeerId(JObjectKey id) {
        if (!id.value().endsWith("_tree_node")) {
            throw new IllegalArgumentException("Not a tree node key: " + id);
        }
        return PeerId.of(id.value().substring(0, id.value().length() - "_tree_node".length()));
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        assert name.equals(peerIdToNodeId(PeerId.of(peerId().value())).toString());
        assert name().equals(name);
        return this;
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return List.of(peerId);
    }
}
