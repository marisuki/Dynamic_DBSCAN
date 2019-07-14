import java.io.Serializable;
import java.util.Collection;
import java.util.Vector;

public class DataPoint implements Serializable {
    public Vector<Double> point;
    public Vector<Integer> classify;
    public boolean core;

    DataPoint(Collection<Double> poi, Double inspect){
        point = (Vector<Double>) poi;
        classify = new Vector<Integer>();
        for(Double d: poi){
            classify.add((int) (d/inspect));
        }
    }
}
