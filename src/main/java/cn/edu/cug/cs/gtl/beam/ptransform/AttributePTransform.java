package cn.edu.cug.cs.gtl.beam.ptransform;

import cn.edu.cug.cs.gtl.common.Property;
import cn.edu.cug.cs.gtl.feature.Attribute;

import javax.annotation.Nullable;

public class AttributePTransform extends GeneralPTransform<Attribute> {
    public AttributePTransform() {
    }

    public AttributePTransform(@Nullable String name) {
        super(name);
    }
}
