package gtl.beam.coder;


import com.google.auto.service.AutoService;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import gtl.geom.Geometry;
import gtl.io.type.StorableNull;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviderRegistrar;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import gtl.io.Storable;

/**
 * A {@code StorableCoder} is a {@link Coder} for a Java class that implements {@link Storable}.
 *
 * <p>To use, specify the coder type on a PCollection:
 * <pre>
 * {@code
 *   PCollection<MyRecord> records =
 *       foo.apply(...).setCoder(StorableCoder.of(MyRecord.class));
 * }
 * </pre>
 *
 * @param <T> the type of elements handled by this coder.
 */
public class StorableCoder<T extends Storable> extends CustomCoder<T> {
    private static final long serialVersionUID = 0L;

    /**
     * Returns a {@code StorableCoder} instance for the provided element class.
     * @param <T> the element type
     */
    public static <T extends Storable> StorableCoder<T> of(Class<T> clazz) {
        return new StorableCoder<>(clazz);
    }

    private final Class<T> type;

    public StorableCoder(Class<T> type) {
        this.type = type;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws IOException {
        value.write(outStream);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T decode(InputStream inStream) throws IOException {
        try {
            if (type.equals(StorableNull.class)) {
                // NullStorable has no default constructor
                return (T) StorableNull.get();
            }
            T t = type.newInstance();
            t.read(inStream);
            return t;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CoderException("unable to deserialize record", e);
        }
    }

    @Override
    public List<Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(this,
                "GTL Storable may be non-deterministic.");
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof StorableCoder)) {
            return false;
        }
        StorableCoder<?> that = (StorableCoder<?>) other;
        return Objects.equals(this.type, that.type);
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    /**
     * Returns a {@link CoderProvider} which uses the {@link StorableCoder} for Hadoop
     * {@link Storable writable types}.
     *
     * <p>This method is invoked reflectively from {@link StorableCoder}.
     */
    public static CoderProvider getCoderProvider() {
        return new StorableCoderProvider();
    }

    /**
     * A {@link CoderProviderRegistrar} which registers a {@link CoderProvider} which can handle
     * {@link Storable writable types}.
     */
    @AutoService(CoderProviderRegistrar.class)
    public static class StorableCoderProviderRegistrar implements CoderProviderRegistrar {

        @Override
        public List<CoderProvider> getCoderProviders() {
            return Collections.singletonList(getCoderProvider());
        }
    }

    /**
     * A {@link CoderProvider} for Hadoop {@link Storable writable types}.
     */
    private static class StorableCoderProvider extends CoderProvider {
        private static final TypeDescriptor<Storable> STORABLE_TYPE = new TypeDescriptor<Storable>() {};

        @Override
        public <T> Coder<T> coderFor(TypeDescriptor<T> typeDescriptor,
                                     List<? extends Coder<?>> componentCoders) throws CannotProvideCoderException {
            if (!typeDescriptor.isSubtypeOf(STORABLE_TYPE)) {
                throw new CannotProvideCoderException(
                        String.format(
                                "Cannot provide %s because %s does not implement the interface %s",
                                StorableCoder.class.getSimpleName(),
                                typeDescriptor,
                                Storable.class.getName()));
            }

            try {
                @SuppressWarnings("unchecked")
                Coder<T> coder = StorableCoder.of((Class) typeDescriptor.getRawType());
                return coder;
            } catch (IllegalArgumentException e) {
                throw new CannotProvideCoderException(e);
            }
        }
    }
}