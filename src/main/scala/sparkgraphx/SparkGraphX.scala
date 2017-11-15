package sparkgraphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

case class DescriptorNode(major: Boolean, uid: String, name: String)

object SparkGraphX extends App {
  val conf = new SparkConf().setAppName("sparkgraphx.SparkGraphX").setMaster("spark://pandora00:7077") //.setMaster("local[*]") 
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val files = "a b c d e f g h".split(" ").map(e => "/data/BigData/Medline/medsamp2016" + e.trim + ".xml").map(readData(_))

  val nodesRDD = sc.parallelize(files.flatMap(_(0)._1))
  val edgesRDD = sc.parallelize(files.flatMap(_(0)._2))
  val graph = Graph(nodesRDD, edgesRDD)

  val majorNodesRDD = sc.parallelize(files.flatMap(_(1)._1))
  val majorEdgesRDD = sc.parallelize(files.flatMap(_(1)._2))
  val majorGraph = Graph(majorNodesRDD, majorEdgesRDD)

  println("/*---------------------(づ｡◕‿‿◕｡)づ---------------------*/")
  
  val numNames = nodesRDD.map(tup => tup._2.name).distinct().count()
  println("Number of distinct names: " + numNames) //22547


  //println("     nodesRDD.count(): " + nodesRDD.count())
  //println("     edgesRDD.count(): " + edgesRDD.count())
  //println("majorNodesRDD.count(): " + majorNodesRDD.count())
  //println("majorEdgesRDD.count(): " + majorEdgesRDD.count())

    //nodesRDD.take(30).foreach(println)
  //majorNodesRDD.take(30).foreach(println)

  val cc = graph.connectedComponents().vertices
  val ccCount = cc.map(_._2).distinct().count()
  cc.groupBy(_._2).mapValues(_.toSeq.length).take(10) foreach println
  
  println("connected components count: " + ccCount) //24 
  //println("size of connected components: " +ccCouple._1 / ccCouple._2) 

  println("/*---------------------(ﾉ ｡◕‿‿◕｡)ﾉ*:･ﾟ✧ ✧ﾟ･-------------*/")
  sc.stop()

  // Data Collection Methods
  def readData(name: String): Array[(Array[(Long, DescriptorNode)], Array[Edge[Unit]])] = {
    val data = xml.XML.loadFile(name)
    val nodes = (data \ "MedlineCitation").map { citation =>
      (citation \\ "DescriptorName").map { descriptor =>
        val major = if ((descriptor \ "@MajorTopicYN").text == "Y") true else false
        val uid = (descriptor \ "@UI").text
        val name = descriptor.text
        new DescriptorNode(major, uid, name)
      }.toArray
    }.toArray.filterNot(_.isEmpty)

    val majorNodes = nodes.map { n =>
      n.filter(_.major)
    }.filterNot(_.isEmpty)

    Array(nodes, majorNodes).map(parseData(_))
  }

  def parseData(nodes: Array[Array[DescriptorNode]]): (Array[(Long, DescriptorNode)], Array[Edge[Unit]]) = {
    val nodesFlat = nodes.flatten.distinct.zipWithIndex.map { case (n, i) => i.toLong -> n }
    val indexMap = nodesFlat.map { case (i, n) => n.name -> i }.toMap

    val edges = nodes.flatMap { citation =>
      citation.combinations(2).flatMap { combo =>
        Seq(Edge(indexMap(combo(0).name), indexMap(combo(1).name), ()), Edge(indexMap(combo(1).name), indexMap(combo(0).name), ()))
      }
    }.toArray

    (nodesFlat, edges)
  }

}
