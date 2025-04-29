package com.usatiuk.utils;

import java.util.List;
import java.util.function.Function;

public class ListUtils {

    public static <T, T_V> List<T_V> prependAndMap(T_V item, List<T> suffix, Function<T, T_V> suffixFn) {
        T_V[] arr = (T_V[]) new Object[suffix.size() + 1];
        arr[0] = item;
        for (int i = 0; i < suffix.size(); i++) {
            arr[i + 1] = suffixFn.apply(suffix.get(i));
        }
        return List.of(arr);
    }

    public static <T> List<T> prepend(T item, List<T> suffix) {
        T[] arr = (T[]) new Object[suffix.size() + 1];
        arr[0] = item;
        for (int i = 0; i < suffix.size(); i++) {
            arr[i + 1] = suffix.get(i);
        }
        return List.of(arr);
    }

    public static <T, T_V> List<T_V> map(List<T> suffix, Function<T, T_V> suffixFn) {
        T_V[] arr = (T_V[]) new Object[suffix.size()];
        for (int i = 0; i < suffix.size(); i++) {
            arr[i] = suffixFn.apply(suffix.get(i));
        }
        return List.of(arr);
    }
}
