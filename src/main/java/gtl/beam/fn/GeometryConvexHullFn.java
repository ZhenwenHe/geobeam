package gtl.beam.fn;

import gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryConvexHullFn<T extends Geometry> extends DoFn<T,Geometry> {
    @ProcessElement
    public void processElement(ProcessContext c){
        Geometry g =  c.element().convexHull();
        c.output(g);
    }
}
