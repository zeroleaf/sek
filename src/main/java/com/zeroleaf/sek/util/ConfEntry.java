package com.zeroleaf.sek.util;

/**
 * @author zeroleaf
 */
public class ConfEntry<V> {

    private final String name;
    private final V value;

    public ConfEntry(String name, V value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public V getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConfEntry)) {
            return false;
        }

        ConfEntry confEntry = (ConfEntry) o;

        return name.equals(confEntry.name)
               && value.equals(confEntry.value);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ConfEntry{" +
               "name='" + name + '\'' +
               ", value='" + value + '\'' +
               '}';
    }
}
