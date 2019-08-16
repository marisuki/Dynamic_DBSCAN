import java.io.Serializable;
import java.util.*;

import static java.lang.Math.sqrt;


// d-dim "quad"tree, 2^d sub-cell/partition
// lower bound=eps*rho/sqrt(d)

public class QuadTree implements Serializable {
    private double eps;
    private double rho;
    private int precise;
    private Node root;
    private Vector<Integer> core;
    QuadTree(Vector<Integer> core, double eps, double rho, int precise){
        this.eps = eps; this.rho = rho; this.precise = precise; this.core = core;
        root = new Node(0, core, new HashMap<>(), 1, eps/(1.0*sqrt(core.size())));
    }
    public int insert(List<Vector<Integer>> points){
        return root.insert(points, precise, eps*rho/sqrt(core.size()));
    }
    public int delete(List<Vector<Integer>> points){
        return root.delete(points, precise);
    }
    public int query(Vector<Double> p){
        //System.out.println(p.get(0));
        return root.intersect(p, eps);
    }
    public int queryRoot(){ return root.value;}
    public String asString(){
        return root.asString();
    }
}
