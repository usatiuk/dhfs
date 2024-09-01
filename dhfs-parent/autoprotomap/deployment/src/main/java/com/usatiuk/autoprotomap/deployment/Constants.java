package com.usatiuk.autoprotomap.deployment;

public class Constants {
    public static final String FIELD_PREFIX = "_";

    public static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static String stripPrefix(String str, String prefix) {
        if (str.startsWith(prefix)) {
            return str.substring(prefix.length());
        }
        return str;
    }


}
