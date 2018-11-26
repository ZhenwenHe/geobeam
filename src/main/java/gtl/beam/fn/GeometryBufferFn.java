package gtl.beam.fn;

import gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryBufferFn<T extends Geometry> extends DoFn<T,Geometry> {
    double distance;

    public GeometryBufferFn(double distance) {
        this.distance = distance;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Geometry g = c.element().buffer(this.distance);
        c.output(g);
    }
}
