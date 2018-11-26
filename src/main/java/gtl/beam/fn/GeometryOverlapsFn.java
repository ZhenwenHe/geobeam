package gtl.beam.fn;

import gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryOverlapsFn<T extends Geometry> extends DoFn<T,Boolean> {
    T geometry;

    public GeometryOverlapsFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Boolean b = this.geometry.overlaps(c.element());
        c.output(b);
    }
}
