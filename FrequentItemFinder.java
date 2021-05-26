import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import org.apache.hadoop.io.Text;

public class FrequentItemFinder {
    public HashSet<HashSet<Integer>> findFreqItem(double thres, Text value) throws IOException {
        HashMap<Integer, Integer> holder = new HashMap<>();
        HashSet<HashSet<Integer>> results = new HashSet<>();
        BufferedReader reader = new BufferedReader(new StringReader(value.toString()));
        for (String sw = reader.readLine(); sw != null; sw = reader.readLine()) {
            for (String w : sw.split("\\s")) {
                holder.merge(Integer.parseInt(w), 1, (a, b) -> a + b);
            }
        }
        for (Integer key : holder.keySet()) {
            if ((float) holder.get(key) >= thres) {
                HashSet<Integer> temp = new HashSet<>();
                temp.add(key);
                results.add(temp);
            }
        }
        return results;
    } //findFreqItem()

    public HashSet<HashSet<Integer>> findFreqSet(double thres, Text value, HashSet<HashSet<Integer>> candidates) throws IOException { //make trans hashset
        HashSet<HashSet<Integer>> results = new HashSet<>();
        HashMap<HashSet<Integer>, Integer> holder = new HashMap<>();
        BufferedReader reader = new BufferedReader(new StringReader(value.toString()));
        for (String sw = reader.readLine(); sw != null; sw = reader.readLine()) {
            HashSet<Integer> transaction = new HashSet<>();
            for (String w : sw.split("\\s")) {
                transaction.add(Integer.parseInt(w));
            }
            for (HashSet<Integer> set : candidates) {
                boolean itemsPresent = true;

                for (Integer item : set) {
                    if (!transaction.contains(item)) {
                        itemsPresent = false;
                        break;
                    }
                }
                if (itemsPresent) {
                    holder.merge(set, 1, (a, b) -> a + b);
                }
            }
        }
        for (HashSet<Integer> key : holder.keySet()) {
            if ((float) holder.get(key) >= thres) {
                results.add(key);
            }
        }
        return results;
    } //findFreqSet()

    public HashMap<HashSet<Integer>, Integer> findSetSupport(Text value, HashSet<HashSet<Integer>> candidates) throws IOException {
        HashMap<HashSet<Integer>, Integer> results = new HashMap<>();
        BufferedReader reader = new BufferedReader(new StringReader(value.toString()));
        for (String sw = reader.readLine(); sw != null; sw = reader.readLine()) {
            HashSet<Integer> transaction = new HashSet<>();
            for (String w : sw.split("\\s")) {

                transaction.add(Integer.parseInt(w));
            }
            for (HashSet<Integer> set : candidates) {
                boolean itemsPresent = true;
                for (Integer item : set) {
                    if (!transaction.contains(item)) {
                        itemsPresent = false;
                        break;
                    }
                }
                if (itemsPresent) {
                    results.merge(set, 1, (a, b) -> a + b);
                }
            }
        }
        return results;
    } //findSetSupport()
} //FrequentItemFinder