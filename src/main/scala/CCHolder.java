import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class CCHolder implements Serializable {
    public Map<Vector<Integer>, Vector<Vector<Double>>> CCPoints = new HashMap<>();
    private GraphCluster gcc;
    private Double partitionLength;
    public CCHolder(Double partitionLength){ this.partitionLength = partitionLength;}

    public void addPoints(Vector<Integer> tar, Vector<Vector<Double>> t){
        if(CCPoints.containsKey(tar)){
            CCPoints.get(tar).addAll(t);
        }
        else{
            CCPoints.put(tar, t);
        }
    }

    
}
