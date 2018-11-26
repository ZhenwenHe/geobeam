package gtl.beam.app;

import gtl.beam.coder.GeometryCoder;
import gtl.beam.coder.StorableCoder;
import gtl.beam.ptransform.EnvelopePTransforms;
import gtl.beam.ptransform.FeaturePTransforms;
import gtl.beam.ptransform.GeometryPTransforms;
import gtl.beam.ptransform.StringPTransforms;
import gtl.common.Identifier;
import gtl.common.Variant;
import gtl.config.Config;
import gtl.data.feature.FeatureStore;
import gtl.data.sdb.beam.BeamDataSolution;
import gtl.data.sdb.beam.feature.BeamFeatureReader;
import gtl.data.sdb.beam.feature.BeamFeatureSet;
import gtl.data.sdb.beam.feature.BeamFeatureStore;
import gtl.feature.Feature;
import gtl.feature.FeatureType;
import gtl.feature.FeatureTypeBuilder;
import gtl.beam.pipeline.SpatialPipeline;
import gtl.filter.EnvelopeContainsFeatureFilter;
import gtl.geom.Envelope;
import gtl.geom.Geometry;
import gtl.geom.Polygon;
import gtl.io.File;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;

public class BeamSDBApp {

    public static void main(String [] args){

        if(args==null ||  args.length<2){
            String[] defaultArgs = new String[3];
            defaultArgs[0]="--inputFile="+ Config.getDataDirectory()+ File.separator+"counties"+File.separator+"county_small.tsv";
            defaultArgs[1]="--outputFile="+Config.getTemporaryDirectory()+File.separator+"county_small";
            defaultArgs[2]="--runner=DirectRunner";
//            defaultArgs[2]="--geometryType=21";
//            defaultArgs[3]="--partitionType=0";
//            defaultArgs[4]="--overwrite=1";
//            defaultArgs[5]="--sampleNumber=100";
            args=defaultArgs;
        }

        BeamSDBOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(BeamSDBOptions.class);
        SpatialPipeline p = new SpatialPipeline(options);

        CoderRegistry cs = p.getPipeline().getCoderRegistry();

        String inputDataFile=options.getInputFile();
        String outputDataFile=options.getOutputFile();

        BeamDataSolution solution = new BeamDataSolution(p.getPipeline());

//        FeatureTypeBuilder ftb = new FeatureTypeBuilder()
//                .setIdentifier(Identifier.create())
//                .setName("example")
//                .setCoordinateReferenceSystem(null)
//                .add("geom",Polygon.class);
//        for(int i=0;i<15;++i)
//            ftb.add(new Integer(i).toString(), Variant.STRING);
//        final FeatureType ft = ftb.build();

        try{
            List<Feature> featureList = null;
            BeamFeatureStore featureStore = (BeamFeatureStore)solution.createFeatureStore("example");
            BeamFeatureSet fs =(BeamFeatureSet) featureStore.createFeatureSetFromTSVFile(p.getPipeline(),inputDataFile);
            //BeamFeatureSet fs =(BeamFeatureSet) featureStore.createFeatureSet(ft);
            //fs.readTSV(p.getPipeline(),inputDataFile);
//            PCollection<Envelope> envelopePCollection = fs.getTotalExtent();
//            envelopePCollection.apply(EnvelopePTransforms.printer());
//            PCollection<Envelope> e = envelopePCollection.apply(EnvelopePTransforms.random(0.01)).setCoder(StorableCoder.of(Envelope.class));
//            e.apply(EnvelopePTransforms.printer());
            //featureList = fs.getData(new EnvelopeContainsFeatureFilter(new Envelope(-130,-100,30,50)));
            BeamFeatureReader reader = (BeamFeatureReader) fs.getDataReader(new EnvelopeContainsFeatureFilter(new Envelope(-130,-100,30,50)));
            //reader.print();
            reader.writeTSV(outputDataFile+"_query.tsv");
            p.getPipeline().run().waitUntilFinish();
//            while (featureList==null) Thread.sleep(1000);
//            for (Feature f : featureList){
//                System.out.println(f.getGeometry().getEnvelope().toString());
//            }
        }
        catch (IOException|InterruptedException e){
            e.printStackTrace();
        }

        p.getPipeline().run().waitUntilFinish();
    }

    public interface BeamSDBOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        /*@Default.String("hdfs://geosciences/data/spatialhadoop/counties/county_small.tsv")*/
        @Default.String("~/gtl/data/dat/counties/county_small.tsv")
        @Validation.Required
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("~/gtl/data/dat/counties/county_small")
        String getOutputFile();
        void setOutputFile(String value);



//        @Description("Geometry Type")
//        @Default.Integer(0)
//        int getGeometryType();
//        void setGeometryType(int value);
//
//        @Description("Partition Type")
//        @Default.Integer(0)
//        int getPartitionType();
//        void setPartitionType(int value);
//
//        @Description("Number of Samples")
//        @Default.Integer(100)
//        int getSampleNumber();
//        void setSampleNumber(int value);
//
//
//        @Description("Overwrite 1 or 0")
//        @Default.Integer(1)
//        int getOverwrite();
//        void setOverwrite(int value);
    }

    void test(Pipeline p){
//        PCollection<Feature> pfs = p.apply(FeaturePTransforms.tsvReader(inputDataFile))
//                .setCoder(StringUtf8Coder.of())
//                .apply(FeaturePTransforms.tsvFilter(geomType))
//                .setCoder(StringUtf8Coder.of())
//                .apply(FeaturePTransforms.parser(FileDataSplitter.TSV.getDelimiter(),ft))
//                .setCoder(StorableCoder.of(Feature.class));
//
//        pfs.apply(FeaturePTransforms.geometry())
//                .setCoder(GeometryCoder.of(Geometry.class))
//                .apply(GeometryPTransforms.wktString())
//                .setCoder(StringUtf8Coder.of())
//                .apply(StringPTransforms.writer(outputDataFile+"1.wkt"));
//
//        pfs.apply(FeaturePTransforms.tsvString())
//                .setCoder(StringUtf8Coder.of())
//                .apply(StringPTransforms.writer(outputDataFile+".tsv"));
//
//
//        p.apply(GeometryPTransforms.tsvReader(inputDataFile))
//                .setCoder(StringUtf8Coder.of())
//                .apply(GeometryPTransforms.tsvFilter(geomType))
//                .setCoder(StringUtf8Coder.of())
//                .apply(GeometryPTransforms.parser(FileDataSplitter.TSV.getDelimiter()))
//                .setCoder(GeometryCoder.of(Geometry.class))
//                .apply(GeometryPTransforms.wktString())
//                .setCoder(StringUtf8Coder.of())
//                .apply(StringPTransforms.writer(outputDataFile+"2.wkt"));
//
//        System.out.println("\n=================================================================\n");
//        p.run().waitUntilFinish();
//

//        p.apply(FeaturePTransforms.tsvReader(inputDataFile))
//                .setCoder(StringUtf8Coder.of())
//                .apply(FeaturePTransforms.tsvFilter(geomType))
//                .setCoder(StringUtf8Coder.of())
//                .apply(FeaturePTransforms.parser(FileDataSplitter.TSV.getDelimiter(),ft))
//                .setCoder(StorableCoder.of(Feature.class))
//                .apply(EnvelopePTransforms.fromFeature())
//                .setCoder(StorableCoder.of(Envelope.class))
//                .apply(EnvelopePTransforms.combiner())
//                .setCoder(StorableCoder.of(Envelope.class))
//                .apply(EnvelopePTransforms.printer());

//        PCollection<String> ps= p.apply(TextIO.read().from(inputDataFile)).apply(
//                org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
//                    @Override
//                    public Boolean apply(String line){
//                        int i = line.indexOf('(');
//                        String tag = line.substring(0,i);
//                        return tag.trim().equalsIgnoreCase(Geometry.getSimpleTypeName(geomType)) ;
//                    }
//                })
//        ).setCoder(StringUtf8Coder.of());
//        PCollection<Feature> pfs=ps.apply(
//                MapElements.via(new SimpleFunction<String, Feature>() {
//                    @Override
//                    public Feature apply(String line){
//                        String[] columns = line.split(FileDataSplitter.TSV.getDelimiter());
//                        final WKTReader wktReader = WKTReader.create();
//                        FeatureBuilder featureBuilder = new FeatureBuilder(ft)
//                                .setIdentifier(Identifier.create())
//                                .setName("")
//                                .add(wktReader.read(columns[0]));
//
//                        for(int i=1;i<columns.length;++i)
//                            featureBuilder.add(columns[i]);
//
//                        return  featureBuilder.build();
//                    }
//                })
//        ).setCoder(StorableCoder.of(Feature.class));
//        final WKTWriter wktWriter = WKTWriter.create(2);
//        pfs.apply(ParDo.of(new DoFn<Feature, Void>() {
//                    @ProcessElement
//                   public void processElement(ProcessContext c){
//                        String pp = (String) wktWriter.write(c.element().getGeometry());
//                        System.out.println(pp);
//                    }
//        }));

//        //Beam 2.7 中下列的写法无法运行，因为其对TextIO已经做了修改，在2.4中可以运行
//        PCollection<String> ps= BeamFeatureSet.readTSV(p.getPipeline(),inputDataFile,Geometry.POLYGON);
//
//        BeamFeatureSet.transform(ps,FileDataSplitter.TSV.getDelimiter(),ft).setCoder(StorableCoder.of(Feature.class))
//                .apply(ParDo.of(new DoFn<Feature, Void>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c){
//                        final WKTWriter wktWriter = WKTWriter.create(2);
//                        String pp = (String) wktWriter.write(c.element().getGeometry());
//                        System.out.println(pp);
//                    }
//                }));
        p.run().waitUntilFinish();
    }
}
