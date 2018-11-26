package gtl.geobeam.ptransform;

import org.junit.Test;

public class SampleTransformsTest {
    @Test
    public void combineFn() throws Exception {
    }

    @Test
    public void anyCombineFn() throws Exception {
    }

    @Test
    public void any() throws Exception {
//        final WKTWriter wktWriter =  WKTWriter.create(2);
//        final ArrayList<Geometry> ga = Geom2DSuits.extractGeometryFromShapeFile("D:\\devs\\data\\DEMO\\DEMO\\maps\\钻孔.SHP");
//
//        String [] args = new String[1];
//        //args[0]="any";
//        // Create the pipeline.
//        PipelineOptions options =
//                PipelineOptionsFactory.fromArgs(args).create();
//        Pipeline p = Pipeline.create(options);
//        GeometryPCollection<Geometry> gpc = new GeometryPCollection(p,ga);
//        //PCollection<Geometry> pc = pcollection.apply(Sample.any(6));
//        gpc.apply(ParDo.<Geometry,Void>of(new DoFn<Geometry, Void>() {
//            @ProcessElement
//            public void processElement(ProcessContext c){
//                System.out.println(wktWriter.write(c.element()));
//            }
//        }));
//
////        while (samples.hasNext()){
////            System.out.println(wktWriter.write(samples.next()));
////        }
//        p.run().waitUntilFinish();
    }

    @Test
    public void fixedSizeGlobally() throws Exception {
    }

    @Test
    public void fixedSizePerKey() throws Exception {
    }

}