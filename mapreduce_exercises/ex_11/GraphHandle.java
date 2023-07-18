package ex_11;

import java.util.*;

public class GraphHandle {
	private Map<String,List<String>> adjVertices;
	
	public GraphHandle() {
		this.adjVertices = new HashMap<>();
	}
	
	public void addVertex(String vertex) {
		adjVertices.putIfAbsent(vertex, new ArrayList<>());
	}
	
	public void addEdge(String src, String des) {
		adjVertices.get(src).add(des);
	}
		
	public List<String> getAllAdjVertices(String vertex){
		return adjVertices.get(vertex);
	}
	
	public Set<String> getAllVertices(){
		return adjVertices.keySet();
	}
	
	public int calculateNumOfConnectedComponent() {
		List<String> connectedcomponent = new ArrayList<String>();
		connectedcomponent.addAll(this.getAllVertices());
		
		int result = 0;
		for (String v: this.getAllVertices()) {
			if (connectedcomponent.contains(v)) {
				boolean flag = true; 
				List<String> neighbors = this.getAllAdjVertices(v);
				for (String n: neighbors) {
					if (!connectedcomponent.contains(n)) {
						flag = false;
					}
					connectedcomponent.remove(n);
				}
				if (flag == true) result++;
				connectedcomponent.remove(v);
			}
		}
		return result;
	}
	// public static void main(String[] args) {
	// 	GraphHandle graph = new GraphHandle();
	// 	graph.addVertex("0");
	// 	graph.addVertex("1");
	// 	graph.addVertex("2");
	// 	graph.addVertex("3");
	// 	graph.addVertex("4");
	// 	graph.addVertex("5");
	// 	graph.addVertex("6");
	// 	graph.addVertex("7");
	// 	graph.addVertex("8");
	// 	graph.addVertex("9");
	// 	graph.addEdge("0","9");
	// 	graph.addEdge("1","4");
	// 	graph.addEdge("1","9");
	// 	graph.addEdge("2","7");
	// 	graph.addEdge("3","5");
	// 	graph.addEdge("3","8");
	// 	graph.addEdge("4","1");
	// 	graph.addEdge("5","3");
	// 	graph.addEdge("7","2");
	// 	graph.addEdge("8","3");
	// 	graph.addEdge("9","0");
	// 	int result = graph.calculateNumOfConnectedComponent();
	// 	System.out.println(result);
	// }
}