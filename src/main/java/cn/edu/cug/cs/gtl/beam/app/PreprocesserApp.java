package cn.edu.cug.cs.gtl.beam.app;

import cn.edu.cug.cs.gtl.beam.coder.StorableCoder;
import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.geom.Polygon;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import cn.edu.cug.cs.gtl.io.wkt.WKTReader;
import cn.edu.cug.cs.gtl.io.wkt.WKTWriter;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public class PreprocesserApp  implements Serializable {


    private static final long serialVersionUID = -4046870457785035608L;


    public static void main(String [] args){

        if(args==null || args.length==0){
            String[] defaultArgs = new String[6];
            defaultArgs[0]="--inputDataFile=d:/gtl/data/dat/counties/county_small.tsv";
            defaultArgs[1]="--outputDataFile=d:/data/county_small";
            defaultArgs[2]="--geometryType=21";
            defaultArgs[3]="--partitionType=0";
            defaultArgs[4]="--overwrite=1";
            defaultArgs[5]="--sampleNumber=100";
            args=defaultArgs;
        }


        PreprocesserOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(PreprocesserOptions.class);
        SpatialPipeline p = new SpatialPipeline(options);


        String inputDataFile=options.getInputDataFile();
        int geomType= options.getGeometryType();
        int samples=options.getSampleNumber();
        String outputDataFile=options.getOutputDataFile();
        int partitionType = options.getPartitionType();

        boolean overwrite = options.getOverwrite()==1;


//        Preprocesser pp =  Preprocesser.create(
//                inputDataFile,geomType,partitionType,samples,outputDataFile,overwrite,p);
//

        final WKTReader wktReader = WKTReader.create();

        //Beam 2.7 中下列的写法无法运行，因为其对TextIO已经做了修改，在2.4中可以运行
        PCollection<String> ps= p.apply(TextIO.read().from(inputDataFile)).apply(
                Filter.by(new SerializableFunction<String,Boolean>() {
                    @Override
                    public Boolean apply(String line){
                        int i = line.indexOf('(');
                        String tag = line.substring(0,i);
                        return tag.trim().equalsIgnoreCase(Geometry.getSimpleTypeName(geomType)) ;
                    }
                })
        ).setCoder(StringUtf8Coder.of());

        ps.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                String pp = (String) c.element();
                System.out.println(pp);
            }
        }));

        PCollection<Polygon> ppc= ps.apply(
                MapElements.via(new SimpleFunction<String, Polygon>() {
                    @Override
                    public Polygon apply(String line){
                        String[] columns = line.split(FileDataSplitter.TSV.getDelimiter());
                        Polygon g=  (Polygon) wktReader.read(columns[0]);
                        return  g;
                    }
                })
        ).setCoder(StorableCoder.of(Polygon.class));

        final WKTWriter wktWriter = WKTWriter.create(2);
        PCollection<String> pss=ppc.apply(
                MapElements.via(new SimpleFunction<Polygon, String>() {
                    @Override
                    public String apply( Polygon g){
                        return wktWriter.write(g);
                    }
                })
        );
        pss.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                String pp = (String) c.element();
                System.out.println(pp);
            }
        }));
        //Beam 2.7 中下列的写法无法运行，因为其对TextIO已经做了修改，在2.4中可以运行
        pss.apply(TextIO.write().to(outputDataFile).withoutSharding());

        p.run().waitUntilFinish();
    }

    public interface PreprocesserOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        /*@Default.String("hdfs://geosciences/data/spatialhadoop/counties/county_small.tsv")*/
        @Default.String("/home/hadoop/Documents/devs/data/spatialhadoop/counties/county_small.tsv")
        @Validation.Required
        String getInputDataFile();
        void setInputDataFile(String value);

        @Description("Path of the file to write to")
        @Default.String("")
        String getOutputDataFile();
        void setOutputDataFile(String value);

        @Description("Geometry Type")
        @Default.Integer(0)
        int getGeometryType();
        void setGeometryType(int value);

        @Description("Partition Type")
        @Default.Integer(0)
        int getPartitionType();
        void setPartitionType(int value);

        @Description("Number of Samples")
        @Default.Integer(100)
        int getSampleNumber();
        void setSampleNumber(int value);


        @Description("Overwrite 1 or 0")
        @Default.Integer(1)
        int getOverwrite();
        void setOverwrite(int value);

    }

}
