package gtl.beam.app;

import gtl.beam.pcollection.IndexablePolygonPCollection;
import gtl.beam.pcollection.Preprocesser;
import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Envelope;
import gtl.index.strtree.DefaultVisitor;
import org.apache.beam.sdk.options.*;

public class GeoBeamApp {
    public interface GeoBeamAppOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        /*@Default.String("hdfs://geosciences/data/spatialhadoop/counties/county_small.tsv")*/
        @Default.String("hdfs://localhost:50070/user/hadoop/data/county_small.tsv")
        @Validation.Required
        String getInputDataFile();
        void setInputDataFile(String value);

        @Description("partitions")
        @Default.Integer(4)
        int getPartitions();
        void setPartitions(int value);

        @Description("Index Type")
        @Default.Integer(0)
        int getIndexType();
        void setIndexType(int value);


        @Description("Index Node Capacity")
        @Default.Integer(32)
        int getIndexCapacity();
        void setIndexCapacity(int value);


        @Description("Leaf Node Capacity")
        @Default.Integer(32)
        int getLeafCapacity();
        void setLeafCapacity(int value);

    }

    public static void main(String [] args){
        GeoBeamAppOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(GeoBeamAppOptions.class);
        SpatialPipeline p = new SpatialPipeline(options);

        String inputDataFile= options.getInputDataFile();

        Preprocesser pp = Preprocesser.load(inputDataFile,p);

        IndexablePolygonPCollection ipc = new IndexablePolygonPCollection(pp,
                options.getPartitions(),
                options.getIndexType(),
                options.getIndexCapacity(),
                options.getLeafCapacity());

        ipc.rangeQuery(Envelope.create(0,0,10,10),new DefaultVisitor());
    }
}
