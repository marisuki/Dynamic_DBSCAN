import java.io.Serializable;
import java.util.*;

public class QuadForest implements Serializable {
    private Map<Vector<Integer>, QuadTree> forest;
    //private Map<Vector<Integer>, Integer> vinCnt; // p->vincnt
    private Map<Vector<Integer>, Set<Vector<Integer>>> globalDic;
    private Set<Vector<Integer>> modified;
    private double eps;
    private double rho;
    private int dim;
    private int precise;
    public QuadForest(double eps, double rho, int dim, int precise){
        this.eps = eps; this.rho = rho; this.dim = dim; this.precise = precise;
        forest = new HashMap<>();
        //vinCnt = new HashMap<>();
        modified = new HashSet<>();
        globalDic = new HashMap<>();
    }
    public void insert(List<Vector<Integer>> points, boolean sameGroup, Vector<Integer> group){
        Map<Vector<Integer>, List<Vector<Integer>>> append = new HashMap<>();
        Vector<Integer> base = new Vector<>();
        if(sameGroup) { base = group; }
        for(Vector<Integer> p: points){
            if(! sameGroup){
                base = new Vector<>();
                for (Integer integer: p) base.add((int) ((integer * 1.0) / (precise*1.0 * (eps / Math.sqrt(dim)))));
            }
            //System.out.println("insert");
            //for(Integer c: base) System.out.print(c); System.out.print(" ");
            //System.out.println();
            if(forest.containsKey(base)){
                if(append.containsKey(base)) append.get(base).add(p);
                else{
                    List<Vector<Integer>> tmp = new ArrayList<>();
                    tmp.add(p);
                    append.put(base, tmp);
                }
            }
            else {
                QuadTree qt = new QuadTree(base, eps, rho, precise);
                forest.put(base, qt);
                List<Vector<Integer>> tmp = new ArrayList<>();
                tmp.add(p);
                append.put(base, tmp);
            }
        }
        for(Vector<Integer> basep: append.keySet()){
            forest.get(basep).insert(append.get(basep));
            modified.add(basep);
            if(globalDic.containsKey(basep)) globalDic.get(basep).addAll(append.get(basep));
            else globalDic.put(basep, new HashSet<>(append.get(basep)));
        }
        //updateVinCntStatus();
    }
    public void delete(List<Vector<Integer>> points, boolean sameGroup, Vector<Integer> group){
        Map<Vector<Integer>, List<Vector<Integer>>> append = new HashMap<>();
        Vector<Integer> base = new Vector<>();
        if(sameGroup) base = group;
        for(Vector<Integer> p: points){
            if(! sameGroup){
                base = new Vector<>();
                for (Integer integer: p) base.add((int) ((integer * 1.0) / (precise*1.0 * (eps / Math.sqrt(dim)))));
            }
            //System.out.println("insert");
            //for(Integer c: base) System.out.print(c); System.out.print(" ");
            //System.out.println();
            if(globalDic.get(base).contains(p)){
                if(append.containsKey(base)) append.get(base).add(p);
                else{
                    List<Vector<Integer>> tmp = new ArrayList<>();
                    tmp.add(p);
                    append.put(base, tmp);
                }
                globalDic.get(base).remove(p);
            }
            else {
                System.out.println("[E] Delete with non-exist QuadTree->Node->Point.");
                System.out.println("[I] Continue deletion, skip error. [POS: QuadForest:delete].");
            }
        }
        for(Vector<Integer> basep: append.keySet()){
            forest.get(basep).delete(append.get(basep));
            modified.add(basep);
        }
    }
    public int query(Vector<Integer> p){
        //return queryNaive(p);

        if(!modified.isEmpty()){
            updateVinCntStatus();
        }
        return queryNaive(p);
    }
    public Map<Vector<Integer>, Integer> cellSizeUpd(){
        Map<Vector<Integer>, Integer> ans = new HashMap<>();
        for(Vector<Integer> cell: modified){
            ans.put(cell, this.forest.get(cell).queryRoot());
        }
        return ans;
    }
    public int queryNaive(Vector<Integer> p){
        int ans = 0;
        Vector<Integer> cell = new Vector<>();
        for(Integer x: p) cell.add((int) (x*1.0/(precise*1.0*(eps/Math.sqrt(dim)))));
        //for(Integer c: cell) System.out.print(c); System.out.print(" ");
        //System.out.println();
        Vector<Double> point = new Vector<>();
        for(Integer x: p) point.add((x*1.0)/(precise*1.0));
        List<Vector<Integer>> cellList = generatePossibleCells(cell);
        for(int i=0;i<cellList.size();i++){
            double dis = eculdian_cells(cellList.get(i), p);
            double r2 = Math.sqrt(Math.pow(eps/(2*Math.sqrt(dim)), 2)*dim);
            if(dis + r2 - eps <= 1e-5){
                //System.out.print(i);
                if(forest.containsKey(cellList.get(i))) ans += forest.get(cellList.get(i)).query(point);
            }
            else if(dis - r2 - eps <= 1e-5){
                //System.out.print(i);
                if(forest.containsKey(cellList.get(i))) ans += forest.get(cellList.get(i)).query(point);
            }
        }

        return ans;
    }
    public int queryCellTot(Vector<Integer> cell){
        if(forest.containsKey(cell)) return forest.get(cell).queryRoot();
        else return 0;
    }
    public Map<Vector<Integer>, Integer> updateVinCntStatus(){
        long st = System.currentTimeMillis();
        //int cnt = 0;
        Map<Vector<Integer>, Integer> ans = new HashMap<>();
        for(Vector<Integer> t: this.modified) {
            List<Vector<Integer>> rc = generatePossibleCells(t);
            //cnt++;
            //if(cnt%100==0) System.out.println("Step: "+cnt+"/"+this.modified.size());
            for(Vector<Integer> r: rc){
                if(!globalDic.containsKey(r)) continue;
                for(Vector<Integer> p: globalDic.get(r)) {
                    //vinCnt.put(p, queryNaive(p));
                    ans.put(p, queryNaive(p));
                }
            }
        }
        //modified.clear();
        long ed = System.currentTimeMillis();
        System.out.println("Status Update Cost: "+(ed-st)+" ms.");
        //System.gc();
        return ans;
    }
    public void clearModified() { modified.clear(); }
    public List<Vector<Integer>> generatePossibleCells(Vector<Integer> cell){
        int rad = (int) (Math.sqrt(dim)+0.5);
        //System.out.println(rad);
        List<Vector<Integer>> ans = new ArrayList<>();
        ans.add(new Vector<>());
        for(int i=0;i<dim;i++){
            List<Vector<Integer>> tmp = new ArrayList<>();
            for(int j=-rad;j<=rad;j++){
                for(Vector<Integer> p: ans){
                    Vector<Integer> x = (Vector<Integer>) p.clone();
                    x.add(j);
                    tmp.add(x);
                }
            }
            ans = tmp;
        }
        //System.out.println(ans.size());
        //for(int i=0;i<ans.size();i++) System.out.println(ans.get(i).get(0));
        List<Vector<Integer>> finalist = new ArrayList<>();
        for(Vector<Integer> p: ans){
            Vector<Integer> tmp = new Vector<>();
            for(int i=0;i<p.size();i++) tmp.add(p.get(i)+cell.get(i));
            finalist.add(tmp);
        }
        return finalist;
    }
    public double eculdian_cells(Vector<Integer> cell1, Vector<Integer> p){
        double ans = 0;
        for(int i=0;i<cell1.size();i++){
            ans += (cell1.get(i)*eps/Math.sqrt(dim)-p.get(i)/(1.0*precise))*(cell1.get(i)*eps/Math.sqrt(dim)-p.get(i)/(1.0*precise));
        }
        //System.out.println(ans);
        return Math.sqrt(ans);
    }
    public String asString(){
        StringBuilder s = new StringBuilder();
        for(Vector<Integer> k: forest.keySet()) s.append(forest.get(k).asString());
        return s.toString();
    }
}
