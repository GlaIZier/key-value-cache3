package ru.glaizier.key.value.cache3.util.function;

/**
 * @author GlaIZier
 */
@FunctionalInterface
public interface WrappedFunction<T, R, E extends Exception> {
    R apply(T t) throws E;
}
