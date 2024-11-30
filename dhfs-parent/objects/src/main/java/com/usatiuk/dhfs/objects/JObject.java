package com.usatiuk.dhfs.objects;

public abstract class JObject {
    protected final JObjectInterface _jObjectInterface;

    public JObject(JObjectInterface jObjectInterface) {
        _jObjectInterface = jObjectInterface;
    }

    public abstract JData getData();
}
