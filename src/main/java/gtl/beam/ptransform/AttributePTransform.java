package gtl.beam.ptransform;

import gtl.common.Property;
import gtl.feature.Attribute;

import javax.annotation.Nullable;

public class AttributePTransform extends GeneralPTransform<Attribute> {
    public AttributePTransform() {
    }

    public AttributePTransform(@Nullable String name) {
        super(name);
    }
}
