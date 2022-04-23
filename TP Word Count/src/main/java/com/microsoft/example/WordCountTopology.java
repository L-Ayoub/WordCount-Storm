package com.microsoft.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.microsoft.example.RandomSentenceSpout;

public class WordCountTopology {

  //Point d'entrée de la topologie
  public static void main(String[] args) throws Exception {
    //Utilisé pour construire la topologie
    TopologyBuilder builder = new TopologyBuilder();
    
    //Add the spout, with a name of 'spout' 18 
    //and parallelism hint of 5 executors
    builder.setSpout("spout", new RandomSentenceSpout(), 5);
    
    //Ajouter le boulon SplitSentence, avec le nom 'split'
    //et indice de parallélisme de 8 exécuteurs
    //shufflegrouping s'abonne au spout et distribue également
    //tuples (phrases) à travers les instances du boulon SplitSentence
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    
    //Ajouter le compteur, avec le nom 'count'
    //et indice de parallélisme de 12 exécuteurs
    //fieldsgrouping s'abonne au split bolt, et
    //assure que le même mot est envoyé à la même instance
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    //creer un configuration
    Config conf = new Config();
    
    // metter false pour désactiver les informations de débogage lorsque
    // s'exécute en production sur un cluster
    conf.setDebug(false);

    // S'il y a des arguments, nous courons sur un cluster
    if (args != null && args.length > 0) {
      
      // indice de parallélisme pour définir le nombre de travailleurs
      conf.setNumWorkers(3);
      
      // soumettre la topologie
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    
// Sinon, nous exécutons localement
    else {
      // Limite le nombre maximum d'exécuteurs pouvant être générés
      //pour un composant à 3
      conf.setMaxTaskParallelism(3);
      
      //LocalCluster est utilisé pour s'exécuter localement
      LocalCluster cluster = new LocalCluster();
      
      
      // soumettre la topologie
      cluster.submitTopology("word-count", conf, builder.createTopology());
      //sleep
      Thread.sleep(10000);
      //shut down the cluster //ferme le cluster
      cluster.shutdown();
    }
  }
}
