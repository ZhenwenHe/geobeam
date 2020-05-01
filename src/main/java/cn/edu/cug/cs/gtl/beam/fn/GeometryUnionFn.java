package cn.edu.cug.cs.gtl.beam.fn;

import cn.edu.cug.cs.gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryUnionFn<T extends Geometry> extends DoFn<T,Geometry> {
    T geometry;

    public GeometryUnionFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Geometry g = this.geometry.union(c.element());
        c.output(g);
    }
}
