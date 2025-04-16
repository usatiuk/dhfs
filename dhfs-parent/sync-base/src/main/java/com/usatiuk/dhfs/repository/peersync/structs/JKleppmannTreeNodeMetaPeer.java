package com.usatiuk.dhfs.repository.peersync.structs;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

//@ProtoMirror(JKleppmannTreeNodeMetaFileP.class)
public class JKleppmannTreeNodeMetaPeer extends JKleppmannTreeNodeMeta {
    private final JObjectKey _peerId;
    private static final byte[] SUFFIX = "_tree_node".getBytes(StandardCharsets.UTF_8);
    private static final ByteString SUFFIX_BS = UnsafeByteOperations.unsafeWrap(SUFFIX);

    public JKleppmannTreeNodeMetaPeer(PeerId id) {
        super(peerIdToNodeId(id).toString());
        _peerId = id.toJObjectKey();
    }

    public static JObjectKey peerIdToNodeId(PeerId id) {
        return JObjectKey.of(id.toJObjectKey().value().concat(SUFFIX_BS));
    }

    public static PeerId nodeIdToPeerId(JObjectKey id) {
        if (!id.value().endsWith(SUFFIX_BS)) {
            throw new IllegalArgumentException("Not a tree node key: " + id);
        }
        return PeerId.of(JObjectKey.of(id.value().substring(0, id.value().size() - SUFFIX_BS.size())));
    }

    public JObjectKey getPeerId() {
        return _peerId;
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        assert name.equals(peerIdToNodeId(PeerId.of(getPeerId())).toString());
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
