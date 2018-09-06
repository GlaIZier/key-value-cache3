package ru.glaizier.key.value.cache3.util.function;

import java.lang.reflect.Constructor;
import java.util.function.Function;

/**
 * @author GlaIZier
 */
public final class Functions {
    private Functions() {
    }

    /**
     * Wraps function to throw some runtime exception
     */
    public static <T, R, E extends Exception> Function<T, R> wrap(WrappedFunction<T, R, E> f,
                                                                  Class<? extends RuntimeException> e) {
        return op -> {
            try {
                return f.apply(op);
            } catch (Exception checked) {
                try {
                    Constructor<? extends RuntimeException> constructor = e
                        .getConstructor(String.class, Throwable.class);
                    throw constructor.newInstance(checked.getMessage(), checked);
                } catch (Exception reflectionEx) {
                    throw new RuntimeException(reflectionEx.getMessage(), reflectionEx);
                }
            }
        };
    }
}
