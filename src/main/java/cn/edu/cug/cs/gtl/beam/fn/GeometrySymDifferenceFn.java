package cn.edu.cug.cs.gtl.beam.fn;

import cn.edu.cug.cs.gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometrySymDifferenceFn<T extends Geometry> extends DoFn<T,Geometry> {
    T geometry;

    public GeometrySymDifferenceFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Geometry g = this.geometry.symDifference(c.element());
        c.output(g);
    }
}
