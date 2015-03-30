package bigdata.twitter;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class WordCounterBolt extends BaseRichBolt {
	
    // Nombre de secondes avant la fin du compte
    private final long logIntervalSec;
    // Nombre de secondes avant la fin de l'intervalle
    private final long clearIntervalSec;
    // Nombre de mots à afficher
    private final int topListSize;

    private OutputCollector collector;
    
    private Map<String, Integer> counts;
    private long lastLogTime;
    private long lastClearTime;
    private int number = 1;

    public WordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
        this.topListSize = topListSize;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	counts = new HashMap<String, Integer>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("trendings", "number"));
    }

    public void execute(Tuple input) {
    	long now = System.currentTimeMillis();
        String word = (String) input.getValueByField("word");

        Integer count = counts.get(word);
        if (count == null){
    	   count = 0;
        }
        count++;
        // count = count == null ? 1L : count + 1;
        // récupère le mot et le count en flux
        counts.put(word, count);
       

        // word : mot , count : total du mot       
        long logPeriodSec = (now - lastLogTime) / 1000;
       
//        if(System.currentTimeMillis() > now + 3000) {
//        	topList(counter);
//        }
       
        if (logPeriodSec > logIntervalSec) {
            System.out.println("Words counted: "+counts.size());
            topList(counts);
            lastLogTime = now;
            //counts.clear();
            number++;
        }
    }
    
    // calculate top list:
    private void topList(Map<String, Integer> counts) {
    			
        SortedMap<Integer, String> trendings = new TreeMap<Integer, String>();
        
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            int count = entry.getValue();
            String word = entry.getKey();
                     
            trendings.put(count, word); // not good put replaces the counts egals
            
            if (trendings.size() > topListSize) {
            	trendings.remove(trendings.firstKey());
            }
        }
        
        System.out.println(number);
        System.out.println("Number of trendings: "+trendings.size());
        collector.emit(new Values(trendings, number)); // envoi vers le prochain bolt pour affichage
    	
        // Remettre à zéro les comptes
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
        	System.out.println("FIN DU COMPTE ON REPREND A ZERO");
            lastClearTime = now;
            counts.clear();
            number = 0;
        }
    }
         
}

//ValueComparator bvc =  new ValueComparator(counts);
//TreeMap<String,Integer> trendings = new TreeMap<String,Integer>(bvc);
//sorted_map.putAll(trends);

//for (Map.Entry<String, Integer> entry : counts.entrySet()) {
//    int count = entry.getValue();
//    String word = entry.getKey();
//
//    trendings.put(word, count); // not good put replaces the counts egals
//    // put(key, value)
//    
//    if (trendings.size() > topListSize) {
//    	trendings.remove(trendings.firstKey());
//    }
//}
//
//System.out.println(trendings);

//class ValueComparator implements Comparator<String> {
//
//    Map<String, Integer> base;
//    public ValueComparator(Map<String, Integer> base) {
//        this.base = base;
//    }
//
//    // Note: this comparator imposes orderings that are inconsistent with equals.    
//    public int compare(String a, String b) {
//        if (base.get(a) >= base.get(b)) {
//            return -1;
//        } else {
//            return 1;
//        } // returning 0 would merge keys
//    }
//}