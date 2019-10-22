import org.apache.spark.{SparkConf, SparkContext}

object depth {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Breadthfirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph=Map(1 -> List(2,4), 2-> List(1,3), 3-> List(2,4), 4-> List(1,3))

    def DFS(start: Vertex, g: Graph): List[Vertex] = {
      def DFS0(vertex: Vertex,visited: List[Vertex]): List[Vertex] = {
        println(vertex)

        println(visited)
        if(visited.contains(vertex)) {
          visited
        }
        else {
          val newNeighbor = g(vertex).filterNot(visited.contains)
          println(newNeighbor)
          newNeighbor.foldLeft(vertex :: visited)((b, a) => DFS0(a, b))
        }
      }

      DFS0(start, List()).reverse
    }

    val dfsresult=DFS(1,g)

    println("DFS Output",dfsresult.mkString(","))
  }
}