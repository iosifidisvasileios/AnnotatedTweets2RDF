
import breeze.numerics.abs
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object RdfExtractor {

  def main(args: Array[String]) {
    val stopwordsLoader: StopwordsLoader = new StopwordsLoader
    val pattern: PatternMatching = new PatternMatching

    def conf = new SparkConf().setAppName(this.getClass.getName)//.setMaster("yarn-client")

    val format = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val formatOutput = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile(args(0))
    val percentage = args(1).toDouble
    val outputDir = args(2)

    val datasetSize = (data.count() * percentage).toInt
    val minimumDataset = sc.parallelize(data.take(datasetSize))

    val header = sc.parallelize(Seq("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
      "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
      "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
      "@prefix nee: <http://www.ics.forth.gr/isl/oae/core#> .",
      "@prefix schema: <http://schema.org/> .",
      "@prefix dc: <http://purl.org/dc/terms/> .",
      "@prefix sioc: <http://rdfs.org/sioc/ns#> .",
      "@prefix sioc_t: <http://rdfs.org/sioc/types#> .",
      "@prefix onyx: <http://www.gsi.dit.upm.es/ontologies/onyx/ns#> .",
      "@prefix wna: <http://www.gsi.dit.upm.es/ontologies/wnaffect/ns#> ."))


    val output = minimumDataset.map { line =>
      val tempMatrix= line.split("\t")
      val tweetId = tempMatrix(0)
      val username = tempMatrix(1)
      val tempDate = format.parse(tempMatrix(2))
      val favorite = tempMatrix(6)
      val retweet = tempMatrix(5)
      val entities = tempMatrix(7)
      val positiveScore = (tempMatrix(8).split(" ")(0).toDouble - 1.0) / 4.0
      val negativeScore = (abs(tempMatrix(8).split(" ")(1).toDouble) - 1.0) / 4.0
      val mentions = tempMatrix(9)
      val hashTags = tempMatrix(10)

      var text_8 = ""
      var text_9 = ""
      var text_10 = ""

      var text_1 = "_:t" + tweetId + " rdf:type sioc:Post ; dc:created \"" + formatOutput.format(tempDate) + "\"^^xsd:dateTime ;" +
        " sioc:id \"" + tweetId + "\" ; sioc:has_creator _:u" + username + " ; onyx:hasEmotionSet _:es"+tweetId+
        "; schema:interactionStatistic _:i" + tweetId + "_1, _:i" + tweetId + "_2 .\n"

      var text_2 = "_:u" + username + " rdf:type sioc:UserAccount ; sioc:id \"" + username + "\" .\n"

      var text_3 = "_:i" + tweetId +"_1 rdf:type schema:InteractionCounter ; " +
        "schema:interactionType schema:LikeAction ; schema:userInteractionCount \"" + favorite + "\"^^xsd:integer .\n"

      var text_4 = "_:i" + tweetId + "_2 rdf:type schema:InteractionCounter ; schema:interactionType schema:ShareAction ;" +
        " schema:userInteractionCount \"" + retweet + "\"^^xsd:integer .\n"

      var text_5 = "_:es" + tweetId + " rdf:type onyx:EmotionSet ; onyx:hasEmotion _:em" + tweetId + "Pos, _:em" + tweetId + "Neg .\n"
      var text_6 = "_:em" + tweetId + "Pos onyx:hasEmotionCategory wna:positive-emotion ; onyx:hasEmotionIntensity \"" + positiveScore + "\"^^xsd:double .\n"
      var text_7 = "_:em" + tweetId + "Neg onyx:hasEmotionCategory wna:negative-emotion ; onyx:hasEmotionIntensity \"" + negativeScore + "\"^^xsd:double .\n"

      if(!entities.equals("null;")){

        val indexEntities = entities.split(";")

        val afterFiltering = indexEntities.filter{x =>
          val entityName = x.split(":")(1)
          !stopwordsLoader.isStopword(entityName) &&
            entityName.length != 1 &&
            !entityName.contains("_film") &&
            !entityName.contains("_song") &&
            !entityName.contains("_series") &&
            !entityName.contains("_album") &&
            !entityName.contains("%28film%29") &&
            !entityName.contains("%28song%29") &&
            !entityName.contains("%28tv_series%29") &&
            !entityName.contains("album%29")
        }

        if(afterFiltering.nonEmpty){

          for ( indexCounter <- afterFiltering.indices) {
            var tempEntity = afterFiltering(indexCounter)
            text_8 += "_:t" + tweetId + " schema:mentions _:e" + tweetId + "_" + indexCounter + " .\n"
            text_8 += "_:e" + tweetId + "_" + indexCounter + " rdf:type nee:Entity ; nee:detectedAs \"" +
              tempEntity.split(":")(0).replaceAll("[^\\x20-\\x7e]", "").replaceAll("\"", "&quot;").replaceAll("'", "&apos;").replace("\\", "\\\\") +
              "\" ; nee:hasMatchedURI <http://dbpedia.org/resource/" +
              tempEntity.split(":")(1).replaceAll("[^\\x20-\\x7e]", "").replaceAll("\"", "&quot;").replaceAll("'", "&apos;").replace("\\", "\\\\") +
              "> ; nee:confidence \"" + tempEntity.split(":")(2) + "\"^^xsd:double .\n"
          }
        }
      }

      if(!mentions.equals("null;")){
        val indexMentions = mentions.split(" ")
         var myMentionList = new ListBuffer[String]()

        for(indexCounter <-0 until indexMentions.length) {
          val moreItems = pattern.hasMore("#" + indexMentions(indexCounter))
          for(i <- 0 until moreItems.size()){
            myMentionList += moreItems.get(i)
          }
        }

        for(indexCounter <- myMentionList.toList.indices) {

          val tempMention = myMentionList(indexCounter).replaceAll("[^\\x20-\\x7e]", "").replaceAll("\"", "&quot;").replaceAll("'", "&apos;").replace("\\", "\\\\")
          if (!tempMention.equals("")) {
            text_9 += "_:t" + tweetId + " schema:mentions _:m" + tweetId + "_" + indexCounter + " .\n"
            text_9 += "_:m" + tweetId + "_" + indexCounter + " rdf:type sioc:UserAccount ; sioc:name \"" +
              tempMention + "\" .\n"
          }
        }
        println(myMentionList.toList)
      }

      if(!hashTags.equals("null;")){
        val indexHashtags = hashTags.split(" ")
        var myHashTagList = new ListBuffer[String]()

        for(indexCounter <-0 until indexHashtags.length) {
          val moreItems = pattern.hasMore("#" + indexHashtags(indexCounter))
          for(i <- 0 until moreItems.size()){
            myHashTagList  += moreItems.get(i)
          }
        }
        for(indexCounter <- myHashTagList.toList.indices) {
          val hashTagTemp = myHashTagList(indexCounter).replaceAll("[^\\x20-\\x7e]", "").replaceAll("\"", "&quot;").replaceAll("'", "&apos;").replace("\\", "\\\\")
          if (!hashTagTemp.equals("")) {
            text_10 += "_:t" + tweetId + " schema:mentions _:h" + tweetId + "_" + indexCounter + " .\n"
            text_10 += "_:h" + tweetId + "_" + indexCounter + " rdf:type sioc_t:Tag ; rdfs:label \"" + hashTagTemp + "\" .\n"
          }
        }

      }

      text_1 + text_2 + text_3 + text_4 + text_5 + text_6 + text_7 + text_8 + text_9 + text_10
    }.cache()
    val withHeader = header ++ output
    //        output.repartition(1).saveAsTextFile("ISWC_Tuples/" + args(0).split("/")(1))

    withHeader.saveAsTextFile(outputDir)
  }

}
