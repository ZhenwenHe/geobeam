package cn.edu.cug.cs.gtl.beam.ptransform;

import cn.edu.cug.cs.gtl.common.Identifier;
import cn.edu.cug.cs.gtl.common.Variant;
import cn.edu.cug.cs.gtl.config.Config;
import cn.edu.cug.cs.gtl.feature.Feature;
import cn.edu.cug.cs.gtl.feature.FeatureBuilder;
import cn.edu.cug.cs.gtl.feature.FeatureType;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import cn.edu.cug.cs.gtl.io.Filter;
import cn.edu.cug.cs.gtl.io.wkt.WKTReader;
import cn.edu.cug.cs.gtl.io.wkt.WKTWriter;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

/**
 *
 */
public class FeaturePTransforms {
    /**
     * 要素过滤器，用于查询
     * <code>
     *     PCollection<Feature> pfs = ......
     *     final Envelop e=......
     *     PCollection<Feature> part = pfs
     *      .apply(new EnvelopeIntersectsFeatureFilter(e))
     *      .setCoder(StorableCoder.of(Feature.class));
     * </code>
     * @param featureFilter
     * @return
     */
    public static FeaturePTransform filter(Filter<Feature> featureFilter){
        return FeaturePTransform.by(featureFilter);
    }

    /**
     * 读取TSV文件,存储为字符串PCollection
     * <code>
     *     Pipeline p;
     *     ....//create pipeline;
     *     PCollection<String> tsvLines = p
     *     .apply( FeaturePTransforms.tsvReader(inputDataFile))
     *     .setCoder(StringUtf8Coder.of());
     * </code>
     * @param inputDataFile
     * @return
     */
    public static  TextIO.Read tsvReader(String inputDataFile){
        return TextIO.read().from(inputDataFile);
    }

    /**
     * 读取CSV文件,存储为字符串PCollection
     * <code>
     *     Pipeline p;
     *     ....//create pipeline;
     *     PCollection<String> csvLines = p
     *     .apply( FeaturePTransforms.csvReader(inputDataFile))
     *     .setCoder(StringUtf8Coder.of());
     * </code>
     * @param inputDataFile
     * @return
     */
    public static  TextIO.Read csvReader(String inputDataFile){
        return TextIO.read().from(inputDataFile);
    }

    /**
     * 读取SSV文件,存储为字符串PCollection
     * <code>
     *     Pipeline p;
     *     ....//create pipeline;
     *     PCollection<String> ssvLines = p
     *     .apply( FeaturePTransforms.ssvReader(inputDataFile))
     *     .setCoder(StringUtf8Coder.of());
     * </code>
     * @param inputDataFile
     * @return
     */
    public static  TextIO.Read ssvReader(String inputDataFile){
        return TextIO.read().from(inputDataFile);
    }

    /**
     * 从TSV字符串集合中提取指定集合类型的字符串
     * <code>
     *     PCollection<String> tsvLines4GeomType = tsvLines
     *     .apply( FeaturePTransforms.tsvFilter(Geometry.POLYGON))
     *     .setCoder(StringUtf8Coder.of())
     * </code>
     * @param geomType
     * @return
     */
    public static  org.apache.beam.sdk.transforms.Filter<String> tsvFilter(int geomType){
        return org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
            @Override
            public Boolean apply(String line){
                int i = line.indexOf('(');
                String tag = line.substring(0,i);
                return tag.trim().equalsIgnoreCase(Geometry.getSimpleTypeName(geomType)) ;
            }
        });
    }

    public static  org.apache.beam.sdk.transforms.Filter<String> tsvFilter(Class<?> geomBinding){
        return org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
            @Override
            public Boolean apply(String line){
                int i = line.indexOf('(');
                String tag = line.substring(0,i);
                return tag.trim().equalsIgnoreCase(geomBinding.getSimpleName()) ;
            }
        });
    }

    /**
     * 从CSV字符串集合中提取指定集合类型的字符串
     * <code>
     *     PCollection<String> tsvLines4GeomType = csvLines
     *     .apply( FeaturePTransforms.csvFilter(Geometry.POLYGON))
     *     .setCoder(StringUtf8Coder.of())
     * </code>
     * @param geomType
     * @return
     */
    public static  org.apache.beam.sdk.transforms.Filter<String> csvFilter(int geomType){
        return org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
            @Override
            public Boolean apply(String line){
                int i = line.indexOf('(');
                String tag = line.substring(0,i).trim();
                if(tag.charAt(0)=='\"')
                    tag=tag.substring(1,tag.length());
                return tag.trim().equalsIgnoreCase(Geometry.getSimpleTypeName(geomType)) ;
            }
        });
    }

    public static  org.apache.beam.sdk.transforms.Filter<String> csvFilter(Class<?> geomBinding){
        return org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
            @Override
            public Boolean apply(String line){
                int i = line.indexOf('(');
                String tag = line.substring(0,i).trim();
                if(tag.charAt(0)=='\"')
                    tag=tag.substring(1,tag.length());
                return tag.trim().equalsIgnoreCase(geomBinding.getSimpleName()) ;
            }
        });
    }
    /**
     * 从SSV字符串集合中提取指定集合类型的字符串
     * <code>
     *     PCollection<String> tsvLines4GeomType = csvLines
     *     .apply( FeaturePTransforms.csvFilter(Geometry.POLYGON))
     *     .setCoder(StringUtf8Coder.of())
     * </code>
     * @param geomType
     * @return
     */
    public static  org.apache.beam.sdk.transforms.Filter<String> ssvFilter(int geomType){
        return org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
            @Override
            public Boolean apply(String line){
                int i = line.indexOf('(');
                String tag = line.substring(0,i).trim();
                if(tag.charAt(0)=='\"')
                    tag=tag.substring(1,tag.length());
                return tag.trim().equalsIgnoreCase(Geometry.getSimpleTypeName(geomType)) ;
            }
        });
    }

    public static  org.apache.beam.sdk.transforms.Filter<String> ssvFilter(Class<?> geomBinding){
        return org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
            @Override
            public Boolean apply(String line){
                int i = line.indexOf('(');
                String tag = line.substring(0,i).trim();
                if(tag.charAt(0)=='\"')
                    tag=tag.substring(1,tag.length());
                return tag.trim().equalsIgnoreCase(geomBinding.getSimpleName()) ;
            }
        });
    }

    /**
     * 从字符串中解析信息构建Feature，例如，从TSV字符串中解析Feature
     * <code>
     *     PCollection<String> tsvLines=.....
     *     final FeatureType featureType=......
     *     PCollection<Feature> pfs = tsvLine
     *      .apply( FeaturePTransforms.parser(FileDataSplitter.TSV.getDelimiter(),featureType))
     *      .setCoder(StorableCoder.of(Feature.class));
     * </code>
     * @param delimiter 字符串的分割符，
     *                  例如FileDataSplitter.TSV.getDelimiter()
     *                  FileDataSplitter.CSV.getDelimiter()等
     * @param featureType
     * @return
     */
    private static MapElements<String, Feature> parser(final String delimiter, final FeatureType featureType){
        return MapElements.via(new SimpleFunction<String, Feature>() {
            @Override
            public Feature apply(String line){
                String[] columns = line.split(delimiter);
                final WKTReader wktReader = WKTReader.create();
                FeatureBuilder featureBuilder = new FeatureBuilder(featureType)
                        .setIdentifier(Identifier.create())
                        .setName("")
                        .add(wktReader.read(columns[0]));

                for(int i=1;i<columns.length;++i)
                    featureBuilder.add(columns[i]);
                Feature f=   featureBuilder.build();
                return f;
            }
        });
    }

    /**
     * 从字符串中解析信息构建Feature，例如，从TSV字符串中解析Feature
     * <code>
     *     PCollection<String> tsvLines=.....
     *     final FeatureType featureType=......
     *     PCollection<Feature> pfs = tsvLine
     *      .apply( FeaturePTransforms.parser(FileDataSplitter.TSV.getDelimiter(),featureType))
     *      .setCoder(StorableCoder.of(Feature.class));
     * </code>
     * @param featureType
     * @return
     */
    public static MapElements<String, Feature> parserTSV(final FeatureType featureType){
        return MapElements.via(new SimpleFunction<String, Feature>() {
            @Override
            public Feature apply(String line){
//                String[] columns = line.split(FileDataSplitter.TSV.getDelimiter());
//                final WKTReader wktReader = WKTReader.create();
//                FeatureBuilder featureBuilder = new FeatureBuilder(featureType)
//                        .setIdentifier(Identifier.create())
//                        .setName("")
//                        .add(wktReader.read(columns[0]));
//
//                for(int i=1;i<columns.length;++i)
//                    featureBuilder.add(columns[i]);
//                Feature f=   featureBuilder.build();
//                return f;
                return Feature.tsvString(line,featureType);
            }
        });
    }

    public static MapElements<String, Feature> parserCSV(final FeatureType featureType){
        return MapElements.via(new SimpleFunction<String, Feature>() {
            @Override
            public Feature apply(String line){
//                String[] columns = line.split(FileDataSplitter.CSV.getDelimiter());
//                final WKTReader wktReader = WKTReader.create();
//                FeatureBuilder featureBuilder = new FeatureBuilder(featureType)
//                        .setIdentifier(Identifier.create())
//                        .setName("")
//                        .add(wktReader.read(columns[0]));
//
//                for(int i=1;i<columns.length;++i)
//                    featureBuilder.add(columns[i]);
//                Feature f=   featureBuilder.build();
//                return f;
                return Feature.csvString(line,featureType);
            }
        });
    }

    public static MapElements<String, Feature> parserSSV(final FeatureType featureType){
        return MapElements.via(new SimpleFunction<String, Feature>() {
            @Override
            public Feature apply(String line){
//                //"\"POLYGON ((-156.309467 20.80195,-156.309092 20.802154,-156.309004 20.802181,-156.308841 20.801928,-156.308764 20.801774,-156.308794 20.801741,-156.308857 20.801705,-156.308952 20.801653,-156.309168 20.801543,-156.309215 20.801532,-156.30942 20.801851,-156.309428 20.801869,-156.309467 20.80195))\"                11057904285             H2030   0       3035    +20.8018570     -156.3091111\n" ;
//                int i = line.lastIndexOf('\"');
//                String geom = line.substring(0,i+1).trim();
//                String attributes = line.substring(i+1).trim();
//                String[] columns = attributes.split("\\s+");
//                final WKTReader wktReader = WKTReader.create();
//                FeatureBuilder featureBuilder = new FeatureBuilder(featureType)
//                        .setIdentifier(Identifier.create())
//                        .setName("")
//                        .add(wktReader.read(geom));
//
//                for(i=0;i<columns.length;++i)
//                    featureBuilder.add(columns[i]);
//                Feature f=   featureBuilder.build();
//                return f;
                return Feature.ssvString(line,featureType);
            }
        });
    }
    /**
     * 将Feature中几何对象转换成WKT字符串，并打印出来
     * 目前只支持2D，主要是WKTWriterImpl只支持2D，还需要扩展3D
     * <code>
     *     PCollection<Feature> pfs = .......
     *     pfs
     *     .apply(FeaturePTransforms.wktPrinter())
     *     .setCoder(StorableCoder.of(Feature.class));
     * </code>
     * @return
     */
    public static ParDo.SingleOutput<Feature, Void> wktPrinter(){
        return ParDo.of(new DoFn<Feature, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                final WKTWriter wktWriter = WKTWriter.create(2);
                String pp = (String) wktWriter.write(c.element().getGeometry());
                System.out.println(pp);
            }
        });
    }


    /**
     * 从将Feature转化成WKT字符串
     * 目前只支持2D，主要是WKTWriterImpl只支持2D，还需要扩展3D
     * <code>
     *     PCollection<Envelope> pes =......
     *     PCollection<String> pss = pes
     *     .apply(FeaturePTransforms.toTSVString())
     *      .setCoder(StringUtf8Coder.of());
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Feature,String> wktString(){
        return MapElements.via(new SimpleFunction<Feature, String>() {
            @Override
            public String apply(Feature g){
                //return WKTWriter.create(2).write(g.getGeometry());
                return Feature.wktString(g);
            }
        });
    }


    /**
     * 从将Feature转化成TSV字符串
     * 目前只支持2D，主要是WKTWriterImpl只支持2D，还需要扩展3D
     * <code>
     *     PCollection<Feature> pes =......
     *     PCollection<String> pss = pes
     *     .apply(FeaturePTransforms.tsvString())
     *      .setCoder(StringUtf8Coder.of());
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Feature,String> tsvString(){
        return MapElements.via(new SimpleFunction<Feature, String>() {
            @Override
            public String apply(Feature g){
//                StringBuilder stringBuilder=new StringBuilder(WKTWriter.create(2).write(g.getGeometry()));
//                if(g.getValues()!=null){
//                    for(Variant v: g.getValues()){
//                        stringBuilder.append(FileDataSplitter.TSV.getDelimiter());
//                        stringBuilder.append(v.toString());
//                    }
//                }
//                //stringBuilder.append("\n");
//                return stringBuilder.toString();
                return Feature.tsvString(g);
            }
        });
    }

    /**
     * 从将Feature转化成CSV字符串
     * 目前只支持2D，主要是WKTWriterImpl只支持2D，还需要扩展3D
     * <code>
     *     PCollection<Feature> pes =......
     *     PCollection<String> pss = pes
     *     .apply(FeaturePTransforms.csvString())
     *      .setCoder(StringUtf8Coder.of());
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Feature,String> csvString(){
        return MapElements.via(new SimpleFunction<Feature, String>() {
            @Override
            public String apply(Feature g){
//                StringBuilder stringBuilder=new StringBuilder(WKTWriter.create(2).write(g.getGeometry()));
//                for(Variant v: g.getValues()){
//                    stringBuilder.append(FileDataSplitter.CSV.getDelimiter());
//                    stringBuilder.append(v.toString());
//                }
//                //stringBuilder.append("\n");
//                return stringBuilder.toString();
                return Feature.csvString(g);
            }
        });
    }

    /**
     * 从将Feature转化成Geometry
     * <code>
     *     PCollection<Feature> pes =......
     *     PCollection<Geometry> pss = pes
     *     .apply(FeaturePTransforms.geometry())
     *     .setCoder(StorableCoder.of(Geometry.class));
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Feature,Geometry> geometry(){
        return MapElements.via(new SimpleFunction<Feature, Geometry>() {
            @Override
            public Geometry apply(Feature g){
                return g.getGeometry();
            }
        });
    }

    /**
     * 计数器，统计PCollection中元素个数
     * <code>
     *     PCollection<Feature> pfs =.......
     *     PCollection<Long> c= pfs.apply(FeaturePTransforms.counter());
     * </code>
     * @return
     */
    public static PTransform<PCollection<Feature>, PCollection<Long>> counter(){
        return  Count.globally();
    }



    /**
     * 从将Feature转化成字符串
     * <code>
     *     PCollection<Feature> pes =......
     *     PCollection<String> pss = pes
     *     .apply(FeaturePTransforms.toByteString())
     *      .setCoder(StringUtf8Coder.of());
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Feature,String> toByteString(){
        return MapElements.via(new SimpleFunction<Feature, String>() {
            @Override
            public String apply(Feature g){
                try {
                    return g.storeToByteString();
                }
                catch (IOException e){
                    e.printStackTrace();
                    return null;
                }
            }
        });
    }

    /**
     * 从字符串中解析信息构建Feature，例如，从TSV字符串中解析Feature
     * <code>
     *     PCollection<String> tsvLines=.....
     *     final FeatureType featureType=......
     *     PCollection<Feature> pfs = tsvLine
     *      .apply( FeaturePTransforms.parser(FileDataSplitter.TSV.getDelimiter(),featureType))
     *      .setCoder(StorableCoder.of(Feature.class));
     * </code>

     * @return
     */
    public static MapElements<String, Feature> fromByteString( ){
        return MapElements.via(new SimpleFunction<String, Feature>() {
            @Override
            public Feature apply(String line){
                 Feature f = new Feature();
                 try {
                     f.loadFromByteString(line);
                 }
                 catch (IOException e){
                     e.printStackTrace();
                 }
                return f;
            }
        });
    }

}
