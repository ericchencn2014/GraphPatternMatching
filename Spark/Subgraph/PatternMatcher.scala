
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** @author  John Miller, Matthew Saltz
 *  @version 1.1
 *  @date    Thu Jul 25 11:28:31 EDT 2013
 *  @see     LICENSE (MIT style license file).
 */

package scalation.graphalytics

// import collection.mutable.Set

import GraphTypes._

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
/** The `PatternMatcher` abstract class serves as a template for implementing
 *  specific algorithms for graph pattern matching.
 *  @param g  the data graph  G(V, E, l) with vertices v in V
 *  @param q  the query graph Q(U, D, k) with vertices u in U
 */
abstract class PatternMatcher (g: Graph, q: Graph)
{
    protected val qRange     = 0 until q.size     // range for query vertices
    protected val CHECK      = 1024               // check progress after this many matches
    protected val LIMIT      = 1E7                // quit after too many matches
    protected val SELF_LOOPS = false              // whether the directed graph has self-loops

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Apply a graph pattern matching algorithm to find the mappings from the
     *  query graph 'q' to the data graph 'g'.  These are represented by a
     *  multi-valued function 'phi' that maps each query graph vertex 'u' to a
     *  set of data graph vertices '{v}'.
     */
    def mappings (): Array [ISet]

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Apply a graph pattern matching algorithm to find subgraphs of data graph
     *  'g' that isomorphically match query graph 'q'.  These are represented
     *  by a set of single-valued bijective functions {'psi'} where each 'psi'
     *  function maps each query graph vertex 'u' to a data graph vertices 'v'.
     */
    def bijections (): Set [Array [Int]]

    //::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    /** Create an initial array of feasible mappings 'phi' from each query
     *  vertex 'u' to the corresponding set of data graph vertices '{v}' whose
     *  label matches 'u's.
     */
    def feasibleMates (): Array [ISet] =
    {
        q.label.map (u_label => g.getVerticesWithLabel (u_label))
    } // feasibleMates

} // PatternMatcher abstract class

