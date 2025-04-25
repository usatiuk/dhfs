package com.usatiuk.dhfs.peersync.structs;

import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.peersync.PeerId;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;

import java.util.Objects;

//@ProtoMirror(JKleppmannTreeNodeMetaFileP.class)
public class JKleppmannTreeNodeMetaPeer extends JKleppmannTreeNodeMeta {
    private final JObjectKey _peerId;

    public JKleppmannTreeNodeMetaPeer(PeerId id) {
        super(peerIdToNodeId(id).value());
        _peerId = id.toJObjectKey();
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

    public JObjectKey getPeerId() {
        return _peerId;
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        assert name.equals(peerIdToNodeId(PeerId.of(getPeerId().value())).toString());
        assert getName().equals(name);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        JKleppmannTreeNodeMetaPeer that = (JKleppmannTreeNodeMetaPeer) o;
        return Objects.equals(_peerId, that._peerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), _peerId);
    }
}
