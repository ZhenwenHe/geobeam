package cn.edu.cug.cs.gtl.data.sdb.beam.texture;

import cn.edu.cug.cs.gtl.common.Identifier;
import cn.edu.cug.cs.gtl.common.PropertySet;
import cn.edu.cug.cs.gtl.data.texture.TextureStore;
import cn.edu.cug.cs.gtl.geom.Texture;
import cn.edu.cug.cs.gtl.io.DataSchema;
import cn.edu.cug.cs.gtl.io.DataSet;
import cn.edu.cug.cs.gtl.io.ServiceInfo;
import org.apache.beam.sdk.values.PCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class BeamTextureStore implements TextureStore {

    PCollection<Texture> texturePCollection=null;

    @Override
    public boolean open(String s) throws IOException {
        return false;
    }

    @Override
    public Texture append(Texture texture) throws IOException {
        return null;
    }

    @Override
    public Texture remove(Identifier identifier) throws IOException {
        return null;
    }

    @Override
    public Texture remove(String s) throws IOException {
        return null;
    }

    @Override
    public Texture find(Identifier identifier) throws IOException {
        return null;
    }

    @Override
    public Texture find(String s) throws IOException {
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

    /*@Override
    public ServiceInfo getInfo() {
        return null;
    }*/

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void createSchema(DataSchema dataSchema) throws IOException {

    }

    /*@Override
    public void updateSchema(String s, DataSchema dataSchema) throws IOException {

    }*/

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
    public DataSet<DataSchema, Texture> getDataSet(String s) throws IOException {
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
