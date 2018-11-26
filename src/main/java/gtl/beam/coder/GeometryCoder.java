package gtl.beam.coder;

import com.google.auto.service.AutoService;
import gtl.geom.Geometry;
import gtl.io.Storable;
import gtl.io.type.StorableNull;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@code GeometryCoder} is a {@link Coder} for a Java class that implements {@link Geometry}.
 *
 * <p>To use, specify the coder type on a PCollection:
 * <pre>
 *     <code>
 *         PCollection<Feature> pfs = ......
 *         pfs.apply(FeaturePTransforms.geometry())
 *                 .setCoder(GeometryCoder.of(Geometry.class))
 *                 .apply(GeometryPTransforms.wktString())
 *                 .setCoder(StringUtf8Coder.of())
 *                 .apply(StringPTransforms.writer(outputDataFile+".wkt"));
 *     </code>
 * </pre>
 *
 * @param <T> the type of elements handled by this coder.
 */
public class GeometryCoder<T extends Geometry> extends CustomCoder<T> {
    private static final long serialVersionUID = 0L;

    /**
     * Returns a {@code GeometryCoder} instance for the provided element class.
     * @param <T> the element type
     */
    public static <T extends Geometry> GeometryCoder<T> of(Class<T> clazz) {
        return new GeometryCoder<>(clazz);
    }

    public static <T extends Geometry> GeometryCoder<T> of( ) {
        return new GeometryCoder(Geometry.class);
    }
    private final Class<T> type;

    public GeometryCoder(Class<T> type) {
        this.type = type;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws IOException {
        byte geomType = (byte) value.getType();
        byte dim = (byte)value.getDimension();
        outStream.write(geomType);
        outStream.write(dim);
        value.write(outStream);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T decode(InputStream inStream) throws IOException {
        byte geomType = (byte) inStream.read();
        byte dim = (byte) inStream.read();
        T t =(T) Geometry.create(geomType,dim);
        t.read(inStream);
        return t;
    }

    @Override
    public List<Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(this,
                "GTL Geometry may be non-deterministic.");
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof GeometryCoder)) {
            return false;
        }
        GeometryCoder<?> that = (GeometryCoder<?>) other;
        return Objects.equals(this.type, that.type);
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    /**
     * Returns a {@link CoderProvider} which uses the {@link GeometryCoder} for Hadoop
     * {@link Geometry writable types}.
     *
     * <p>This method is invoked reflectively from {@link GeometryCoder}.
     */
    public static CoderProvider getCoderProvider() {
        return new GeometryCoderProvider();
    }

    /**
     * A {@link CoderProviderRegistrar} which registers a {@link CoderProvider} which can handle
     * {@link Geometry writable types}.
     */
    @AutoService(CoderProviderRegistrar.class)
    public static class GeometryCoderProviderRegistrar implements CoderProviderRegistrar {

        @Override
        public List<CoderProvider> getCoderProviders() {
            return Collections.singletonList(getCoderProvider());
        }
    }

    /**
     * A {@link CoderProvider} for Hadoop {@link Geometry writable types}.
     */
    private static class GeometryCoderProvider extends CoderProvider {
        private static final TypeDescriptor<Geometry> GEOMTRY_TYPE = new TypeDescriptor<Geometry>() {};

        @Override
        public <T> Coder<T> coderFor(TypeDescriptor<T> typeDescriptor,
                                     List<? extends Coder<?>> componentCoders) throws CannotProvideCoderException {
            if (!typeDescriptor.isSubtypeOf(GEOMTRY_TYPE)) {
                throw new CannotProvideCoderException(
                        String.format(
                                "Cannot provide %s because %s does not implement the interface %s",
                                GeometryCoder.class.getSimpleName(),
                                typeDescriptor,
                                Geometry.class.getName()));
            }

            try {
                @SuppressWarnings("unchecked")
                Coder<T> coder = GeometryCoder.of((Class) typeDescriptor.getRawType());
                return coder;
            } catch (IllegalArgumentException e) {
                throw new CannotProvideCoderException(e);
            }
        }
    }
}