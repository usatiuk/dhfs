package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.kleppmanntree.NodeMeta;

import java.util.Objects;

//@ProtoMirror(JKleppmannTreeNodeMetaP.class)
public abstract class JKleppmannTreeNodeMeta implements NodeMeta {
    private final String _name;

    public JKleppmannTreeNodeMeta(String name) {
        _name = name;
    }

    public String getName() {
        return _name;
    }

    public abstract JKleppmannTreeNodeMeta withName(String name);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JKleppmannTreeNodeMeta that = (JKleppmannTreeNodeMeta) o;
        return Objects.equals(_name, that._name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_name);
    }

    @Override
    public String toString() {
        return "JKleppmannTreeNodeMeta{" +
                "class=" + this.getClass().getSimpleName() + " " +
                "_name='" + _name + '\'' +
                '}';
    }
}
