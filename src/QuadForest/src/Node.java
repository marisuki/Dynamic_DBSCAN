//import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;

class Node implements Serializable {
    Integer value;
    private Map<Integer, Node> child;
    private Vector<Integer> center;
    //private Node par;
    private double length;
    private int level; // leaf=0, else=1
    Node(int val, Vector<Integer> core, Map<Integer, Node> domain, int level, double unit_len){
        this.value = val;
        this.center = core;
        this.child = domain;
        //this.par = parent;
        this.level = level;
        this.length = unit_len;
    }
    int insert(List<Vector<Integer>> points, int precise, double minimum_unit_length){
        value += points.size();
        if(level == 0) return value;
        Map<Integer, List<Vector<Integer>>> append = new HashMap<>();
        for(Vector<Integer>p: points){
            int val = 0;
            for(int i=0;i<p.size();i++) val += ((center.get(i)*length*(precise*1.0)-p.get(i)<1e-5? 1:0)<<i);
            if(child.containsKey(val)){
                if(append.containsKey(val)) append.get(val).add(p);
                else {
                    List<Vector<Integer>> tmp = new ArrayList<>();
                    tmp.add(p);
                    append.put(val, tmp);
                }
            }
            else{
                Vector<Integer> core = new Vector<>();
                for(int i=0;i<center.size();i++) core.add((int) (p.get(i)/(precise*length/2.0)));
                int level = (length/2.0-minimum_unit_length<=1e-5)? 0:1;
                Node cnn = new Node(0, core, new HashMap<>(), level, length/2.0);
                child.put(val, cnn);
                List<Vector<Integer>> tmp = new ArrayList<>();
                tmp.add(p);
                append.put(val, tmp);
            }
        }
        for(Integer k: append.keySet()){
            child.get(k).insert(append.get(k), precise, minimum_unit_length);
        }
        return value;
    }
    int delete(List<Vector<Integer>> points, int precise){
        // hold that all the deleting points in the appending list: POINTS is actually inside the cell.
        //, which is kept by the outside function and DO NOT examine in Node. (for better performance on mem).
        value -= points.size();
        if(level == 0) return value;
        Map<Integer, List<Vector<Integer>>> append = new HashMap<>();
        for(Vector<Integer> p: points){
            int val = 0;
            for(int i=0;i<p.size();i++) val += ((center.get(i)*length*precise*1.0- p.get(i)<1e-5? 1:0)<<i);
            if(child.containsKey(val)){
                if(append.containsKey(val)) append.get(val).add(p);
                else{
                    List<Vector<Integer>> tmp = new ArrayList<>();
                    tmp.add(p);
                    append.put(val, tmp);
                }
            }
            else{
                System.out.println("[E] Deletion with non-exist cells (points)!");
                System.out.println("[I] Continue deletion, skip error. [POS: Node.delete]");
            }
        }
        for(Integer key: append.keySet()) child.get(key).delete(append.get(key), precise);
        return value;
    }
    int intersect(Vector<Double> p, double eps){
        //System.out.print(p.get(0));
        Vector<Double> tmp = new Vector<>();
        for (Integer integer: center) tmp.add(integer * length);
        double dis = eculdian(tmp, p);
        double r2 = Math.sqrt(Math.pow((length/2.0), 2)*p.size()*1.0);//Math.pow(length, tmp.size())*1.0/(1<<tmp.size())*1.0;
        if(eps-(r2+dis)>=1e-5){
            return value;
        }
        else if(dis - (r2+eps)<=1e-5){
            if(level == 1){
                int ans = 0;
                for(Integer x: child.keySet()) ans += child.get(x).intersect(p, eps);
                return ans;
            }
            else return value;
        }
        return 0;
    }
    private double eculdian(Vector<Double> from, Vector<Double> to){
        double ans = 0;
        for(int i=0;i<from.size();i++) ans += (from.get(i)-to.get(i))*(from.get(i)-to.get(i));
        return Math.sqrt(ans);
    }
    public String asString(){
        StringBuilder ans = new StringBuilder();
        ans.append("value: " + value.toString() + "\n");
        ans.append("path: ");
        for(Integer i: center) ans.append(i.toString()).append(" ");
        ans.append("\n");
        ans.append("child: \n");
        for(Integer k: child.keySet()) ans.append(child.get(k).asString() + "\n");
        return ans.toString();
    }
}
