package gtl.beam.fn;

import gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryDifferenceFn<T extends Geometry> extends DoFn<T,Geometry> {
    T geometry;

    public GeometryDifferenceFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Geometry g = this.geometry.difference(c.element());
        c.output(g);
    }
}
