package com.usatiuk.dhfs.objects.repository.peersync.structs;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;

import java.util.Objects;

//@ProtoMirror(JKleppmannTreeNodeMetaFileP.class)
public class JKleppmannTreeNodeMetaPeer extends JKleppmannTreeNodeMeta {
    private final JObjectKey _peerId;

    public JKleppmannTreeNodeMetaPeer(PeerId id) {
        super(peerIdToNodeId(id).name());
        _peerId = id.toJObjectKey();
    }

    public JObjectKey getPeerId() {
        return _peerId;
    }

    public static JObjectKey peerIdToNodeId(PeerId id) {
        return JObjectKey.of(id.toJObjectKey().name() + "_tree_node");
    }

    public static PeerId nodeIdToPeerId(JObjectKey id) {
        if (!id.name().endsWith("_tree_node")) {
            throw new IllegalArgumentException("Not a tree node key: " + id);
        }
        return PeerId.of(id.name().substring(0, id.name().length() - "_tree_node".length()));
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        assert name.equals(peerIdToNodeId(PeerId.of(getPeerId().name())).toString());
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
