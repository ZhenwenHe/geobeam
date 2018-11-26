package gtl.data.sdb.beam.style;

import gtl.common.Identifier;
import gtl.common.PropertySet;
import gtl.data.styling.StyleStore;
import gtl.io.DataSchema;
import gtl.io.DataSet;
import gtl.io.ServiceInfo;
import gtl.styling.Style;
import org.apache.beam.sdk.values.PCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class BeamStyleStore implements StyleStore {

    PCollection<Style> stylePCollection=null;

    @Override
    public boolean open(String s) throws IOException {
        return false;
    }

    @Override
    public Style append(Style style) throws IOException {
        return null;
    }

    @Override
    public Style remove(Identifier identifier) throws IOException {
        return null;
    }

    @Override
    public Style find(Identifier identifier) throws IOException {
        return null;
    }

    @Override
    public Style find(String s) throws IOException {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public PropertySet getProperties() {
        return null;
    }

    @Override
    public void setProperties(PropertySet propertySet) {

    }

    @Override
    public ServiceInfo getInfo() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void createSchema(DataSchema dataSchema) throws IOException {

    }

    @Override
    public void updateSchema(String s, DataSchema dataSchema) throws IOException {

    }

    @Override
    public void removeSchema(String s) throws IOException {

    }

    @Override
    public String[] getSchemaNames() throws IOException {
        return new String[0];
    }

    @Override
    public List<DataSchema> getSchemas() throws IOException {
        return null;
    }

    @Override
    public DataSet<DataSchema, Style> getDataSet(String s) throws IOException {
        return null;
    }

    @Override
    public Object clone() {
        return null;
    }

    @Override
    public boolean load(DataInput dataInput) throws IOException {
        return false;
    }

    @Override
    public boolean store(DataOutput dataOutput) throws IOException {
        return false;
    }
}
