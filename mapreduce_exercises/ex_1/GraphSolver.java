package ex_1;

import java.util.*;

public class GraphSolver {
  private Map<String,List<String>> Vector;
  
  public GraphSolver() {
	  this.Vector = new HashMap<>();
  }
  
  public void addVertex(String vertex) {
	  Vector.putIfAbsent(vertex, new ArrayList<>());
  }
  
  public void addEdge(String src,String des) {
	  Vector.get(src).add(des);
  }
  
  public Set<String> getAllVertice(){
	  return Vector.keySet();
  }
  
  public List<String> getAllDes(String src){
	  return Vector.get(src);
	  
  }
  
  public String getType_Vertex(String vertex) {
	  int count_src = 0;
	  int count_des = 0;
	  
	  for (String v: this.getAllVertice()) {
		  if (v.equals(vertex)) {
			  count_src = this.getAllDes(v).size();
		  }
		  else {
			  List<String> allDes = this.getAllDes(v);
			  if (allDes.contains(vertex)) {
				  count_des++;
			  }
		  }
	  }
	   if (count_des - count_src < 0) {
          return "pos";
      } else if (count_des - count_src > 0) {
          return "neg";
      } else {
          return "eq";
      }

  }
// I'm using Eclipse to test this
//   public static void main(String[] args) {
// 	  GraphSolver graph = new GraphSolver();
// 	  graph.addVertex("A");
// 	  graph.addVertex("B");
// 	  graph.addVertex("C");
// 	  graph.addVertex("D");
// 	  graph.addVertex("E");
// 	  graph.addEdge("A", "B");
// 	  graph.addEdge("B", "C");
// 	  graph.addEdge("A", "C");
// 	  graph.addEdge("D","E");
	  
// 	  for (String v: graph.getAllVertice()) {
// 		  String type = graph.getType_Vertex(v);
// 		  System.out.println(v + " " + type);
// 	  }
	  
//   }
}