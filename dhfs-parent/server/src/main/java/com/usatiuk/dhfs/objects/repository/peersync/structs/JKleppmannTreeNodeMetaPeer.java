package com.usatiuk.dhfs.objects.repository.peersync.structs;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;

import java.util.Objects;

//@ProtoMirror(JKleppmannTreeNodeMetaFileP.class)
public class JKleppmannTreeNodeMetaPeer extends JKleppmannTreeNodeMeta {
    private final JObjectKey _peerId;

    public JKleppmannTreeNodeMetaPeer(PeerId id) {
        super(id.toString());
        _peerId = id.toJObjectKey();
    }

    public JObjectKey getPeerId() {
        return _peerId;
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        assert false;
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
