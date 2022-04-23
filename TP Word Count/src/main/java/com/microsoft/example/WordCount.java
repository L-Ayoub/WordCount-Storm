package com.microsoft.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;

// For logging
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

// Il existe une variété de types de boulons. Dans ce cas, utilisez BaseBasicBolt
public class WordCount extends BaseBasicBolt {
  
  //Créer un logger pour cette class
  private static final Logger logger = LogManager.getLogger(WordCount.class);
  
  // Pour contenir des mots et des chiffres
  Map<String, Integer> counts = new HashMap<String, Integer>();
  
  // À quelle fréquence émettre un décompte de mots
  private Integer emitFrequency;

  // constructor
  public WordCount() {
      emitFrequency=5; // Default to 60 seconds
  }

  // // Constructeur qui définit la emit frequency
  public WordCount(Integer frequency) {
      emitFrequency=frequency;
  }

  // Configurer la fréquence des tuples de tick pour ce boulon
  // Ceci délivre un tuple 'tick' sur un intervalle spécifique,
  // qui est utilisé pour déclencher certaines actions
  @Override
  public Map<String, Object> getComponentConfiguration() {
      Config conf = new Config();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
      return conf;
  }

  //execute est appelé pour traiter les tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    // Si c'est un tuple de tick, émet tous les mots et count
    if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
      for(String word : counts.keySet()) {
        Integer count = counts.get(word);
        collector.emit(new Values(word, count));
        logger.info("Emitting a count of " + count + " for word " + word);
      }
    } else {
      
      /// Récupère le contenu du mot à partir du tuple
      String word = tuple.getString(0);
      //count
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      //Increment the count and store it
      count++;
      counts.put(word, count);
    }
  }

  // Déclare que ceci émet un tuple contenant deux champs ; mot et count
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count"));
  }
}
