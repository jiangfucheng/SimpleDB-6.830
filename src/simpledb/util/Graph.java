package simpledb.util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * Date: 2019/12/19
 * Time: 15:39
 * 邻接表建图
 *
 * @author jiangfucheng
 */
public class Graph<T> {
    //key表示顶点，value表示该顶点所有的出度
    private Map<T, List<T>> h;
    //记录顶点的入度
    private Map<T, Integer> d;
    //存储所有顶点
    private Set<T> v;

    public Graph() {
        h = new HashMap<>();
        d = new HashMap<>();
        v = new HashSet<>();
    }

    public void add(T from, T to) {
        if (from.equals(to)) return;
        List<T> outs = h.computeIfAbsent(from, k -> new LinkedList<>());
        outs.add(to);
        Integer inCount = d.getOrDefault(to, 0);
        v.add(from);
        v.add(to);
        //d.putIfAbsent(from, 0);
        d.put(to, inCount + 1);
    }

    public void remove(T from, T to) {
        List<T> outs = h.get(from);
        if (outs == null) return;
        outs.removeIf(out -> out.equals(to));
        d.put(to, d.get(to) - 1);
        if (d.get(to) == 0) {
            if (h.get(to) == null || h.get(to).size() == 0)
                v.remove(to);
        }
    }

    //把该结点涉及到的入边和出边都删掉
    public void remove(T node) {
        List<T> ts = h.get(node);
        if (ts != null)
            for (T t : ts) {
                d.put(t, d.get(t) - 1);
            }
        v.remove(node);
        h.remove(node);
        d.remove(node);

        for (Map.Entry<T, List<T>> entry : h.entrySet()) {
            entry.getValue().removeIf(tid -> tid.equals(node));
        }

    }

    public boolean hasCircle() {
        Queue<T> queue = new LinkedList<>();
        Map<T, Integer> backup = new HashMap<>(d);
        int offerCount = 0;
        //把所有入度为0的点加到队列中
        for (T node : v) {
            int inCount = backup.computeIfAbsent(node, n -> 0);
            if (inCount == 0) {
                queue.offer(node);
                offerCount++;
            }
        }
        while (queue.size() > 0) {
            T t = queue.poll();
            List<T> list = h.get(t);
            if (list == null) continue;
            for (T out : list) {
                Integer inCount = backup.get(out);
                backup.put(out, inCount - 1);
                if (backup.get(out) == 0) {
                    queue.offer(out);
                    offerCount++;
                }
            }
        }
        return v.size() != offerCount;
    }


}
