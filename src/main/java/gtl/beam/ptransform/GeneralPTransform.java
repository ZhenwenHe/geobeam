package gtl.beam.ptransform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;

public class GeneralPTransform<T> extends PTransform<PCollection<T>,PCollection<T>> {
    public GeneralPTransform() {
    }

    public GeneralPTransform(@Nullable String name) {
        super(name);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return null;
    }
}
