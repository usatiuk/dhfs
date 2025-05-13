package com.usatiuk.utils;

import java.util.List;
import java.util.function.Function;

/**
 * Utility class for list operations.
 */
public class ListUtils {

    /**
     * Prepends an item to a list and maps the rest of the list using a provided function.
     *
     * @param item     The item to prepend.
     * @param suffix   The list to append to.
     * @param suffixFn The function to map the suffix items.
     * @param <T>      The type of the items in the list.
     * @param <T_V>    The type of the mapped items.
     * @return A new list with the prepended item and mapped suffix items.
     */
    public static <T, T_V> List<T_V> prependAndMap(T_V item, List<T> suffix, Function<T, T_V> suffixFn) {
        T_V[] arr = (T_V[]) new Object[suffix.size() + 1];
        arr[0] = item;
        for (int i = 0; i < suffix.size(); i++) {
            arr[i + 1] = suffixFn.apply(suffix.get(i));
        }
        return List.of(arr);
    }

    /**
     * Prepends an item to a list.
     *
     * @param item   The item to prepend.
     * @param suffix The list to append to.
     * @param <T>    The type of the items in the list.
     * @return A new list with the prepended item and the original suffix items.
     */
    public static <T> List<T> prepend(T item, List<T> suffix) {
        T[] arr = (T[]) new Object[suffix.size() + 1];
        arr[0] = item;
        for (int i = 0; i < suffix.size(); i++) {
            arr[i + 1] = suffix.get(i);
        }
        return List.of(arr);
    }

    /**
     * Maps a list using a provided function.
     *
     * @param suffix   The list to map.
     * @param suffixFn The function to map the items.
     * @param <T>      The type of the items in the list.
     * @param <T_V>    The type of the mapped items.
     * @return A new list with the mapped items.
     */
    public static <T, T_V> List<T_V> map(List<T> suffix, Function<T, T_V> suffixFn) {
        T_V[] arr = (T_V[]) new Object[suffix.size()];
        for (int i = 0; i < suffix.size(); i++) {
            arr[i] = suffixFn.apply(suffix.get(i));
        }
        return List.of(arr);
    }
}
