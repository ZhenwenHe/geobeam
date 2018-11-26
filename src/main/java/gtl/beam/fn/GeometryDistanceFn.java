package gtl.beam.fn;

import gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryDistanceFn<T extends Geometry> extends DoFn<T,Double> {
    T  geometry;

    public GeometryDistanceFn(final T g) {
        geometry=g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        double d= c.element().distance(this.geometry);
        c.output(d);
    }
}
