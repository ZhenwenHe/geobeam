package gtl.beam.ptransform;

import gtl.beam.pcollection.FeatureTypePCollection;
import gtl.common.Identifier;
import gtl.common.Variant;
import gtl.feature.AttributeDescriptor;
import gtl.feature.Feature;
import gtl.feature.FeatureType;
import gtl.feature.FeatureTypeBuilder;
import gtl.geom.Geometry;
import gtl.io.FileDataSplitter;
import gtl.io.wkt.WKTWriter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.POutput;

import java.io.IOException;


public class FeatureTypePTransforms {
    /**
     * 从字符串中解析信息构建FeatureType，例如，从TSV字符串中解析FeatureType
     * <code>
     *     PCollection<String> tsvLines=.....
     *     PCollection<FeatureType> pfs = tsvLine
     *      .apply( FeatureTypePTransforms.parser(schemaName,FileDataSplitter.TSV.getDelimiter()))
     *      .setCoder(StorableCoder.of(FeatureType.class));
     * </code>
     ** @param schemaName
     * @param delimiter 字符串的分割符，
     *                  例如FileDataSplitter.TSV.getDelimiter()
     *                  FileDataSplitter.CSV.getDelimiter()等

     * @return
     */
    public static MapElements<String, FeatureType> parser(final String schemaName, final String delimiter){
        return MapElements.via(new SimpleFunction<String, FeatureType>() {
            @Override
            public FeatureType apply(String input) {
                FeatureTypeBuilder ftb = new FeatureTypeBuilder()
                        .setIdentifier(Identifier.create())
                        .setName(schemaName)
                        .setCoordinateReferenceSystem(null);
                String[] columns = input.split(FileDataSplitter.TSV.getDelimiter());
                String []geomInfo = columns[0].split(FileDataSplitter.PERCENT.getDelimiter());
                assert geomInfo.length==2;
                ftb.add(geomInfo[0], Geometry.getTypeBinding(Geometry.getType(geomInfo[1])));
                for(int i=1;i<columns.length;++i)
                    ftb.add(columns[i], Variant.STRING);
                final FeatureType ft = ftb.build();
                return ft;
            }
        });
    }

    /**
     * 将FeatureType转化tab分割的字符串
     * @return
     */
    public static   MapElements<FeatureType,String> string(){
        return MapElements.via(new SimpleFunction<FeatureType, String>() {
            @Override
            public String apply(FeatureType g){
                StringBuilder stringBuilder=new StringBuilder(
                        g.getIdentifier().toString()+"\t"
                                +g.getName()+"\t"
                                +g.getGeometryDescriptor().getName()+"\t"
                                +g.getGeometryDescriptor().getGeometryType().getBinding().getSimpleName()
                );
                for(AttributeDescriptor v: g.getAttributeDescriptors()){
                    stringBuilder.append("\t") ;
                    stringBuilder.append(g.getName()) ;
                    stringBuilder.append("\t") ;
                    stringBuilder.append(v.getAttributeType().getType());
                }
                stringBuilder.append("\n");
                return stringBuilder.toString();
            }
        });
    }

    /**
     * 将FeatureType转化TSV字符串
     * <code>
     *     PCollection<FeatureType> pfs = ......
     *     PCollection<String> psTSV = pfs
     *      .apply( FeatureTypePTransforms.toTSVString())
     *      .setCoder(StringUTF8Coder.of());
     * </code>
     * @return
     */
    /**
     *
     * @return
     */
    public static MapElements<FeatureType,String> toTSVString(){
        return MapElements.via(new SimpleFunction<FeatureType, String>() {
            @Override
            public String apply(FeatureType g){
                return FeatureType.tsvString(g);
            }
        });
    }

    /**
     * 将TSV字符串转化FeatureType
     * <code>
     *     final String schemaName =......
     *     PCollection<String> psTSV = ......
     *     PCollection<FeatureType> pft = psTSV
     *      .apply( FeatureTypePTransforms.fromTSVString())
     *      .setCoder(StorableCoder.of(FeatureType.class));
     * </code>
     * @return
     */
    public static MapElements<String,FeatureType> fromTSVString(){
        return MapElements.via(new SimpleFunction<String, FeatureType>() {
            @Override
            public FeatureType apply(String input) {
                return FeatureType.tsvString(input);
            }
        });
    }

    /**
     * 将FeatureType转化字符串
     * <code>
     *     PCollection<FeatureType> pfs = ......
     *     PCollection<String> psTSV = pfs
     *      .apply( FeatureTypePTransforms.toByteString())
     *      .setCoder(StringUTF8Coder.of());
     * </code>
     * @return
     */
    /**
     *
     * @return
     */
    public static MapElements<FeatureType,String> toByteString(){
        return MapElements.via(new SimpleFunction<FeatureType, String>() {
            @Override
            public String apply(FeatureType g){
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
     * 将字符串转化FeatureType
     * <code>
     *     final String schemaName =......
     *     PCollection<String> psTSV = ......
     *     PCollection<FeatureType> pft = psTSV
     *      .apply( FeatureTypePTransforms.fromByteString())
     *      .setCoder(StorableCoder.of(FeatureType.class));
     * </code>
     * @return
     */
    public static MapElements<String,FeatureType> fromByteString(){
        return MapElements.via(new SimpleFunction<String, FeatureType>() {
            @Override
            public FeatureType apply(String input) {
                try {
                    FeatureType ft = new FeatureType();
                    ft.loadFromByteString(input);
                    return ft;
                }
               catch (IOException e){
                    e.printStackTrace();
                    return null;
               }
            }
        });
    }
}