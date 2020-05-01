package cn.edu.cug.cs.gtl.beam.partitioning;

import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.util.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class HilbertPartitioning  <E extends Envelope>
        extends SpatialPartitioning<E> implements Serializable {

    private static final long serialVersionUID=1L;
    
    private static final int GRID_RESOLUTION = Integer.MAX_VALUE;


    /** The partitionIdentifiers. */
    protected int[] partitionIdentifiers;


    /**
     * Instantiates a new hilbert partitioning.
     *
     * @param samples the sample list
     * @param totalExtent the total totalExtent
     * @param partitions the partitions
     * @throws Exception the exception
     */
    public HilbertPartitioning(E totalExtent, List<E> samples, int partitions) throws Exception
    {
        super(totalExtent);
        //this.totalExtent=totalExtent;
        int[] hValues = new int[samples.size()];
        for (int i = 0; i < samples.size(); i++){
            hValues[i] = computeHValue(totalExtent, samples.get(i));
        }

        createFromHValues(hValues, partitions);

        // Aggregate samples by partition; compute bounding box of all the samples in each partition
        Envelope [] gridWithoutID = new Envelope[partitions];
        for (Envelope sample : samples) {
            int partitionID = gridID(totalExtent, sample, partitionIdentifiers);
            Envelope current = gridWithoutID[partitionID];
            if (current == null) {
                gridWithoutID[partitionID] = sample;
            } else {
                gridWithoutID[partitionID] = updateEnvelope(current, sample);
            }
        }

        for (Envelope envelope : gridWithoutID) {
            this.partitionEnvelopes.add(envelope);
        }
    }

    public HilbertPartitioning(E totalExtent, Iterable<E> samples, int partitions) throws Exception {
        this(totalExtent, ArrayUtils.<E>iterableToList(samples),partitions);
    }
    /**
     *
     * @return the ID array of the partitions
     */
    public int[] getPartitionIdentifiers() {
        return partitionIdentifiers;
    }

    /**
     * Creates the from H values.
     *
     * @param hValues the h values
     * @param partitions the partitions
     */
    protected void createFromHValues(int[] hValues, int partitions) {
        Arrays.sort(hValues);

        this.partitionIdentifiers = new int[partitions];
        int maxH = 0x7fffffff;
        for (int i = 0; i < partitionIdentifiers.length; i++) {
            int quantile = (int) ((long)(i + 1) * hValues.length / partitions);
            this.partitionIdentifiers[i] = quantile == hValues.length ? maxH : hValues[quantile];
        }
    }

    /**
     * Compute H value.
     *
     * @param n the n
     * @param x the x
     * @param y the y
     * @return the int
     */
    public static int computeHValue(int n, int x, int y) {
        int h = 0;
        for (int s = n/2; s > 0; s/=2) {
            int rx = (x & s) > 0 ? 1 : 0;
            int ry = (y & s) > 0 ? 1 : 0;
            h += s * s * ((3 * rx) ^ ry);

            // Rotate
            if (ry == 0) {
                if (rx == 1) {
                    x = n-1 - x;
                    y = n-1 - y;
                }

                //Swap x and y
                int t = x; x = y; y = t;
            }
        }
        return h;
    }

    /**
     * Location mapping.
     *
     * @param axisMin the axis min
     * @param axisLocation the axis location
     * @param axisMax the axis max
     * @return the int
     */
    public static int locationMapping(double axisMin, double axisLocation, double axisMax)
    {
        Double gridLocation = (axisLocation-axisMin) * GRID_RESOLUTION / (axisMax-axisMin);
        return gridLocation.intValue();
    }


    /**
     * Grid ID.
     *
     * @param totalExtent the totalExtent
     * @param spatialObject the spatial object
     * @param partitionBounds the partition bounds
     * @return the int
     * @throws Exception the exception
     */
    public static int gridID(Envelope totalExtent, Envelope spatialObject, int[] partitionBounds) throws Exception
    {
        int hValue = computeHValue(totalExtent, spatialObject);
        int partition = Arrays.binarySearch(partitionBounds, hValue);
        if (partition < 0) {
            partition = -partition - 1;
        }
        return partition;
    }

    private static int computeHValue(Envelope totalExtent, Envelope spatialObject) {
        int x = locationMapping(totalExtent.getMinX(), totalExtent.getMaxX(),(spatialObject.getMinX() + spatialObject.getMaxX())/2.0);
        int y = locationMapping(totalExtent.getMinY(), totalExtent.getMaxY(),(spatialObject.getMinY() + spatialObject.getMaxY())/2.0);
        return computeHValue(GRID_RESOLUTION+1, x, y);
    }

    /**
     * Update envelope.
     *
     * @param envelope the envelope
     * @param spatialObject the spatial object
     * @return the envelope
     * @throws Exception the exception
     */
    public static Envelope updateEnvelope(Envelope envelope, Envelope spatialObject) throws Exception
    {
        double minX = Math.min(envelope.getMinX(), spatialObject.getMinX());
        double maxX = Math.max(envelope.getMaxX(), spatialObject.getMaxX());
        double minY = Math.min(envelope.getMinY(), spatialObject.getMinY());
        double maxY = Math.max(envelope.getMaxY(), spatialObject.getMaxY());

        return new Envelope(minX, maxX, minY, maxY);
    }

}
