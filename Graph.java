
/**
 * Taken from https://gist.github.com/imamhidayat92/dff60e5554020bd58b64
 * With some modification
 *
 * Credit to imamhidayat92
 * and Ray Andrew <raydreww@gmail.com>
 */

import java.util.*;

/**
 * A simple undirected and unweighted graph implementation.
 *
 * @param <Integer> The type that would be used as vertex.
 */
public class Graph {
  final private HashMap<Integer, Set<Integer>> adjacencyList;
  final private int MAX_SIZE = 4000000;

  /**
   * Create new Graph object.
   */
  public Graph() {
    this.adjacencyList = new HashMap(MAX_SIZE);
  }

  /**
   * Add new vertex to the graph.
   *
   * @param v The vertex object.
   */
  public void addVertex(Integer v) {
    if (!this.adjacencyList.containsKey(v)) {
      this.adjacencyList.put(v, new HashSet<Integer>());
    }
  }

  /**
   * Remove the vertex v from the graph.
   *
   * @param v The vertex that will be removed.
   */
  public void removeVertex(Integer v) {
    if (!this.adjacencyList.containsKey(v)) {
      throw new IllegalArgumentException("Vertex doesn't exist.");
    }

    this.adjacencyList.remove(v);

    for (Integer u : this.getAllVertices()) {
      this.adjacencyList.get(u).remove(v);
    }
  }

  /**
   * Add new edge between vertex. Adding new edge from u to v will automatically
   * add new edge from v to u since the graph is undirected.
   *
   * @param v Start vertex.
   * @param u Destination vertex.
   */
  public void addEdge(Integer v, Integer u) {
    if (!this.adjacencyList.containsKey(v) || !this.adjacencyList.containsKey(u)) {
      // throw new IllegalArgumentException();
      this.addVertex(u);
      this.addVertex(v);
    }

    this.adjacencyList.get(v).add(u);
    this.adjacencyList.get(u).add(v);
  }

  /**
   * Remove the edge between vertex. Removing the edge from u to v will
   * automatically remove the edge from v to u since the graph is undirected.
   *
   * @param v Start vertex.
   * @param u Destination vertex.
   */
  public void removeEdge(Integer v, Integer u) {
    if (!this.adjacencyList.containsKey(v) || !this.adjacencyList.containsKey(u)) {
      throw new IllegalArgumentException();
    }

    this.adjacencyList.get(v).remove(u);
    this.adjacencyList.get(u).remove(v);
  }

  /**
   * Check adjacency between 2 vertices in the graph.
   *
   * @param v Start vertex.
   * @param u Destination vertex.
   * @return <tt>true</tt> if the vertex v and u are connected.
   */
  public boolean isAdjacent(Integer v, Integer u) {
    return this.adjacencyList.get(v).contains(u);
  }

  /**
   * Get all vertices in the graph.
   *
   * @return An Iterable for all vertices in the graph.
   */
  public Iterable<Integer> getAllVertices() {
    return this.adjacencyList.keySet();
  }

  /**
   * Get connected vertices of a vertex.
   *
   * @param v The vertex.
   * @return An iterable for connected vertices.
   */
  public Iterable<Integer> getNeighbors(Integer v) {
    return this.adjacencyList.get(v);
  }

  /**
   * Get connected vertices of a vertex.
   *
   * @param v The vertex.
   * @return Degree count for vertex v.
   */
  public long degree(Integer v) {
    long counter = 0;
    Iterator<Integer> neighborIterator = getNeighbors(v).iterator();

    while (neighborIterator.hasNext()) {
      neighborIterator.next();
      counter++;
    }

    return counter;
  }

  /**
   * Get first neighbor with min grade
   *
   * @param vertices List of vertices.
   * @param v        vertex.
   * @return first neighbor with min grade.
   */
  private Integer firstNeighborIndex(List<Integer> vertices, Integer v) { // fni
    int index, min = vertices.size();
    Iterator<Integer> neighborIterator = getNeighbors(v).iterator();

    while (neighborIterator.hasNext()) {
      index = vertices.indexOf(neighborIterator.next());
      if (min > index) {
        min = index;
      }
    }

    return min;
  }

  /**
   * Get first neighbor with min grade of specific vertex
   *
   * @param vertices List of vertices.
   * @param v        vertex.
   * @param j        vertex to be compared with.
   * @return first neighbor with min grade of vertex v.
   */
  private Integer firstNeighborWithMinDegree(List<Integer> vertices, Integer v, Integer j) { // nni
    int index, min = vertices.size();
    Iterator<Integer> neighborIterator = getNeighbors(v).iterator();

    while (neighborIterator.hasNext()) {
      index = vertices.indexOf(neighborIterator.next());

      if (min > index && index > j) {
        min = index;
      }
    }

    return min;
  }

  public double countTrianglesWithPartition(Integer p) {
    if (p <= 1) {
      throw new Error("Partition must be more than 1");
    }

    List<Integer> vertices = new ArrayList(MAX_SIZE);
    Iterator<Integer> verticesIterator = getAllVertices().iterator();
    verticesIterator.forEachRemaining(vertices::add);

    java.util.Collections.sort(vertices, new java.util.Comparator<Integer>() {

      @Override
      public int compare(Integer o1, Integer o2) {
        return (degree(o1) > degree(o2) ? -1 : (degree(o1) == degree(o2) ? 0 : 1));
      }
    });

    // algorithm compact-forward
    double counter = 0.0;

    Integer l;
    Iterator<Integer> neighborsIterator;
    Integer templ;
    Integer tempk;
    Integer tempi;
    for (int i = 0; i < vertices.size(); i++) {
      neighborsIterator = getNeighbors(vertices.get(i)).iterator();
      while (neighborsIterator.hasNext()) {
        l = vertices.indexOf(neighborsIterator.next());

        if (l < i) {
          Integer j = firstNeighborIndex(vertices, vertices.get(i));
          Integer k = firstNeighborIndex(vertices, vertices.get(l));
          while ((j < l) && (k < l)) {
            if (j < k) {
              j = firstNeighborWithMinDegree(vertices, vertices.get(i), j);
            } else {
              if (k < j) {
                k = firstNeighborWithMinDegree(vertices, vertices.get(l), k);
              } else {
                templ = vertices.get(l);
                tempk = vertices.get(k);
                tempi = vertices.get(i);

                if (templ % p == tempk % p && tempk % p == tempi % p) {
                  counter = counter + (1.0 / (p - 1));

                } else {
                  counter = counter + 1.0;
                } // counting of the others

                j = firstNeighborWithMinDegree(vertices, tempi, j);
                k = firstNeighborWithMinDegree(vertices, templ, k);

              }
            }
          }
        }
      }
    }
    return counter;
  }

  // public static void main(String[] args) {
  // Graph graph = new Graph();

  // graph.addEdge(1, 2);
  // graph.addEdge(2, 1);
  // graph.addEdge(2, 3);
  // graph.addEdge(3, 4);
  // graph.addEdge(4, 3);
  // graph.addEdge(3, 1);
  // graph.addEdge(13, 12);
  // graph.addEdge(12, 13);
  // graph.addEdge(12, 14);
  // graph.addEdge(12, 15);
  // graph.addEdge(14, 15);
  // graph.addEdge(16, 17);

  // Iterator<Integer> graphIterator = graph.getAllVertices().iterator();
  // while (graphIterator.hasNext()) {
  // System.out.println(graphIterator.next());
  // }

  // System.out.println(graph.countTrianglesWithPartition(2));
  // }
}