package cn.edu.cug.cs.gtl.beam.ptransform;

import cn.edu.cug.cs.gtl.geom.Geometry;

import javax.annotation.Nullable;

public class GeometryPTransform <T extends Geometry> extends GeneralPTransform<T> {
    public GeometryPTransform() {
    }

    public GeometryPTransform(@Nullable String name) {
        super(name);
    }
}
