package bigdata.twitter;

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
    // Nombre de mots � afficher
    private final int topListSize;
    // Spring frame pour les r�sultats

    private OutputCollector collector;
    
    private Map<String, Integer> counts;
    private long lastLogTime;
    private long lastClearTime;

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
    	outputFieldsDeclarer.declare(new Fields("trendings"));
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
        // r�cup�re le mot et le count en flux
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
        }
    }
    
    // calculate top list:
    private void topList(Map<String, Integer> counts) {
        SortedMap<Integer, String> trendings = new TreeMap<Integer, String>();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            int count = entry.getValue();
            String word = entry.getKey();
  
//           trendings = addTrending(trendings, count, word);
//            if (trendings.containsKey(count) == true){
//            	System.out.println("Ce compte existe !!!!!");
//            }
//                       
            trendings.put(count, word); // not good put replaces the counts egals
            
            if (trendings.size() > topListSize) {
            	trendings.remove(trendings.firstKey());
            }
        }
         
        collector.emit(new Values(trendings)); // envoi vers le prochain bolt pour affichage
    	
//      WordCloud cloud = new WordCloud(trendings, frame);
//    	cloud.showCloud();

        // Remettre � z�ro les comptes
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
        	System.out.println("FIN DU COMPTE ON REPREND A ZERO");
            counts.clear();
            lastClearTime = now;
        }
    }
    
//  private SortedMap<Integer, String> addTrending(SortedMap<Integer, String> trendings, int count, String word) {
//	
//    if (trendings.containsKey(count) == true){
//    	System.out.println(count);
//    	System.out.println("OUPS");
//    	count = count + 1;
//    	System.out.println("Le nouveau compte est "+count);
//    	addTrending(trendings, count, word);
//    }
//               
//    trendings.put(count, word); // not good put replaces the counts egals
//    if (trendings.size() > topListSize) {
//    	trendings.remove(trendings.firstKey());
//    }
//    return trendings;
//}
    
}
