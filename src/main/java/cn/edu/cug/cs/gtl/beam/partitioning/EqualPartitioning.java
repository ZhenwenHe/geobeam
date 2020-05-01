package cn.edu.cug.cs.gtl.beam.partitioning;

import cn.edu.cug.cs.gtl.geom.Envelope;
import java.io.Serializable; 

public class EqualPartitioning <E extends Envelope>
        extends SpatialPartitioning<E> implements Serializable {

    private static final long serialVersionUID=1L;

    public EqualPartitioning(E totalExtent,int partitions) {
        super(totalExtent);
        assert totalExtent.getDimension()>=2;
        //Local variable should be declared here
        Double root=Math.sqrt(partitions);
        int partitionsAxis;
        double intervalX;
        double intervalY;
        double lows[] = totalExtent.getLowCoordinates();
        double highs[] = totalExtent.getHighCoordinates();
        //Calculate how many bounds should be on each axis
        partitionsAxis=root.intValue();
        intervalX=(highs[0]-lows[0])/partitionsAxis;
        intervalY=(highs[1]-lows[1])/partitionsAxis;
        //System.out.println("totalExtent: "+totalExtent+"root: "+root+" interval: "+intervalX+","+intervalY);
        for(int i=0;i<partitionsAxis;i++)
        {
            for(int j=0;j<partitionsAxis;j++)
            {
                Envelope e=new Envelope(lows[0]+intervalX*i,lows[0]+intervalX*(i+1),lows[1]+intervalY*j,lows[1]+intervalY*(j+1));
                this.partitionEnvelopes.add(e);
            }
        }

    }
}
