
import networkx as nx
#import matplotlib.pyplot as plt


def init_nodes(graph, query):

    labels = [query.node[v]['label'] for v in query]
    print "the label in query is %s " % labels

    for node in graph.nodes_iter():

        # Create the match flag and set its value according to labels in Query Graph
        reset_node(graph, node)

        if graph.node[node]['label'] in labels:
            graph.node[node]['match'] = True

            # make MatchSet for each vertex
            graph.node[node]['match_set'] = [ v for v in query if query.node[v]['label'] == graph.node[node]['label'] ]
        else:
            continue

    for node in graph.nodes_iter():
        for parent in graph.predecessors_iter(node):
            if graph.node[parent]['match'] == True:
                if graph.node[parent]['label'] in graph.node[node]['parent_label_set']:
                    continue
                else:
                    graph.node[node]['parent_label_set'].append(graph.node[parent]['label'])

        graph.node[node]['index'] = node
        print graph.node[node]


def reset_node(graph, node):
    graph.node[node]['match'] = False
    graph.node[node]['match_set'] = []
    graph.node[node]['parent_label_set'] = []


def informed_node(data, node_in_data, node_in_match_set, inform):
    data.node[node_in_data]['match_set'].remove(node_in_match_set)
    print 'node:%d remove matchset %d' % (node_in_data,node_in_match_set)
    inform.append(node_in_data)
    print 'add inform %d' % node_in_data


def compare_parent_condition(data, query, node_in_data, inform):
    for node_in_match_set in data.node[node_in_data]['match_set']:
        temp=False

        for parent in query.predecessors_iter(node_in_match_set):
            if query.node[parent]['label'] not in data.node[node_in_data]['parent_label_set']:
                informed_node(data,node_in_data,node_in_match_set, inform)



def compare_child_condition(data, query, node_in_data, inform):

    for node_in_match_set in data.node[node_in_data]['match_set']:
        temp=False

        for child in data.neighbors_iter(node_in_data):

            if data.node[child]['match'] != True:
                continue

            for query_child in data.node[child]['match_set']:
                if query_child in query.neighbors_iter(node_in_match_set):
                    temp = True

        if temp == False:
            informed_node(data,node_in_data,node_in_match_set, inform)

    if len(data.node[node_in_data]['match_set']) ==0 :
        reset_node(data, node_in_data)
        print 'node:%d reset1 match false' % node_in_data
        return False
    else:
        return True



#if node in data has no child but the matched node in query has child, return False
#if node in data has child but the condition in query is oppsite, return False
def node_does_have_child(data, node_in_data, query, inform):

    child_list=[]
    for child in data.neighbors_iter(node_in_data):

        # if this node don't have any valid child
        if data.node[child]['match'] != True:
            print "node: %d is break at child %d"  %  (node_in_data, child)
            continue

        child_list.append(child)


    if len(child_list) == 0:
        for node_in_query in data.node[node_in_data]['match_set']:

            # if the node has no child but the node which matches in query has child,any vertex has child, remove matchSet,,
            if len(query.neighbors(node_in_query)) != 0:
                informed_node(data,node_in_data,node_in_query, inform)

    else:
        for node_in_query in data.node[node_in_data]['match_set']:

            # if the node has child but the node which matches in query has no child, return False,
            if len(query.neighbors(node_in_query)) == 0:
                informed_node(data,node_in_data,node_in_query, inform)

    if len(data.node[node_in_data]['match_set']) ==0:
        reset_node(data,node_in_data)
        return False
    else:
        return True


#if node in data has no parent but the matched node in query has parent, return False
#if node in data has parent but the condition in query is oppsite, return False
def node_does_have_parent(data, node_in_data, query, inform):

    parent_list=[]
    for parent in data.predecessors_iter(node_in_data):

        # if this node don't have any valid child
        if data.node[parent]['match'] != True:
            print "node: %d is break at parent %d"  %  (node_in_data, parent)
            continue

        parent_list.append(parent)

    if len(parent_list) == 0:
        for node_in_query in data.node[node_in_data]['match_set']:

            # if the node has no child but the node which matches in query has child, return False,
            if len(query.predecessors(node_in_query)) != 0:
                informed_node(data,node_in_data,node_in_query, inform)

    else:
        for node_in_query in data.node[node_in_data]['match_set']:

            # if the node has child but the node which matches in query has no child, return False,
            if len(query.predecessors(node_in_query)) == 0:
                informed_node(data,node_in_data,node_in_query, inform)

    if len(data.node[node_in_data]['match_set']) ==0:
        reset_node(data,node_in_data)
        return False

    # recollect the label of parents
    data.node[node_in_data]['parent_label_set']=[]
    for parent in data.predecessors_iter(node_in_data):
        if data.node[parent]['match'] == True:
            if data.node[parent]['label'] in data.node[node_in_data]['parent_label_set']:
                continue
            else:
                data.node[node_in_data]['parent_label_set'].append(data.node[parent]['label'])

    return True



def dual_simulation(data, query):

    init_nodes(data, query)

    # contain the node which has changes need to informed others
    inform = []

    for node in data.nodes_iter():


        if data.node[node]['match'] != True:
            print "data is break at %d" % node
            continue

        if node_does_have_child(data, node, query, inform) == False:
            continue

        if node_does_have_parent(data, node, query, inform) == False:
            continue

        if compare_child_condition(data,query,node,inform) == False:
            continue

        if compare_parent_condition(data,query,node,inform) == False:
            continue

    while True:

        if len(inform) > 0:
            for node in inform:
                inform.remove(node)

                for parent in data.predecessors_iter(node):

                    if data.node[parent]['match'] == False:
                        print 'parent is False'
                        continue


                    #detect all children and parents from the parent
                    if node_does_have_child(data, node, query, inform) == False:
                        continue

                    if node_does_have_parent(data, node, query, inform) == False:
                        continue

                    if compare_child_condition(data,query,node,inform) == False:
                        continue

                    if compare_parent_condition(data,query,node,inform) == False:
                        continue
        else:
            break

    # for node in data.nodes_iter():
    #     print data.node[node]

def strict_simulation(data, query):

    dual_simulation(data, query)

    qg = query.to_undirected()
    diameter_query = nx.diameter(qg)
    print "Diameter:%d " % diameter_query

    radius_for_ball = diameter_query/2+1
    print "Shortest_path_len:%d" % radius_for_ball

    dg = data.to_undirected()

    result_data_graph = nx.DiGraph()
    result_list =[]

    #create a ball for each vertex centered at itself
    for node in data.nodes_iter():
        temp_graph = nx.DiGraph()

        if data.node[node]['match'] == False:
            continue

        path=nx.single_source_shortest_path_length(dg,node, radius_for_ball)
        print "shortest_path:%s" % path

        list_subgraph = [v for v in path if data.node[v]['match']==True]
        print "list:%s" % list_subgraph
        temp_graph = data.subgraph(list_subgraph)

        print "edges:%s" % temp_graph.edges()


        print "nodes:%s " % temp_graph.nodes()

        dual_simulation(temp_graph, query)

        temp_nodes = [i for i in list_subgraph if temp_graph.node[i]['match']==True]


        if len(temp_nodes)<=0:
            continue

        print "the ball centered %d, mem:%s" % (node, temp_nodes)

        for member_in_ball in temp_graph.nodes_iter():
            if temp_graph.node[member_in_ball]['match'] != True:
                # temp_graph.remove_node(member_in_ball)
                continue
            else:
                if temp_graph.node[member_in_ball]['index'] in result_list:
                    continue
                else:
                    result_list.append(temp_graph.node[member_in_ball]['index'])




        #result_data_graph = nx.union(temp_graph, result_data_graph)

    print "result list %s" % result_list

    # for node in result_data_graph.nodes_iter():
    #     print result_data_graph.node[node]





if __name__ == '__main__':
    DG=nx.DiGraph()
    QG=nx.DiGraph()

    # DG.add_edge(1,2)
    # DG.add_edge(2,1)
    # DG.add_edge(2,3)
    # DG.add_edge(3,4)
    # DG.add_edge(4,5)
    # DG.node[1]['label'] = 'A'
    # DG.node[2]['label'] = 'B'
    # DG.node[3]['label'] = 'A'
    # DG.node[4]['label'] = 'B'
    # DG.node[5]['label'] = 'C'
    # QG.add_edge(1,2)
    # QG.add_edge(2,1)
    # QG.node[1]['label'] = 'A'
    # QG.node[2]['label'] = 'B'
    # QG.add_node(3)
    # QG.node[3]['label'] = 'D'
    # QG.add_node(4)
    # QG.node[4]['label'] = 'A'

    DG.add_edge(1,2)
    DG.add_edge(1,7)
    DG.add_edge(7,8)
    DG.add_edge(3,4)
    DG.add_edge(3,9)
    DG.add_edge(9,4)
    DG.add_edge(6,4)
    DG.add_edge(5,6)
    DG.add_edge(11,4)
    DG.add_edge(11,5)
    DG.add_edge(6,12)
    DG.add_edge(12,11)
    DG.add_edge(13,14)
    DG.add_edge(14,15)
    DG.add_edge(15,16)
    DG.add_edge(14,10)
    DG.add_edge(16,10)
    DG.add_edge(16,13)
    DG.node[1]['label'] = 'HR'
    DG.node[2]['label'] = 'BIO'
    DG.node[3]['label'] = 'HR'
    DG.node[4]['label'] = 'BIO'
    DG.node[5]['label'] = 'AI'
    DG.node[6]['label'] = 'DM'
    DG.node[7]['label'] = 'SE'
    DG.node[8]['label'] = 'BIO'
    DG.node[9]['label'] = 'SE'
    DG.node[10]['label'] = 'BIO'
    DG.node[11]['label'] = 'DM'
    DG.node[12]['label'] = 'AI'
    DG.node[13]['label'] = 'AI'
    DG.node[14]['label'] = 'DM'
    DG.node[15]['label'] = 'AI'
    DG.node[16]['label'] = 'DM'
    DG.add_edge(2,3)
    DG.add_edge(8,9)


    QG.add_edge(1,2)
    QG.add_edge(2,4)
    QG.add_edge(1,4)
    QG.add_edge(3,5)
    QG.add_edge(5,3)
    QG.add_edge(5,4)
    QG.node[1]['label'] = 'HR'
    QG.node[2]['label'] = 'SE'
    QG.node[3]['label'] = 'AI'
    QG.node[4]['label'] = 'BIO'
    QG.node[5]['label'] = 'DM'

    #dual_simulation(DG, QG)
    strict_simulation(DG, QG)


#    print list(nx.simple_cycles(DG))
#    print "Diameter:%d " % nx.diameter(DG.to_undirected())





#    g = nx.disjoint_union(DG, QG)
#    node_color = [float(DG.node[v]['label']) for v in DG]
#    nx.draw_networkx(g)
#    plt.show()

 #   node_color = [float(QG.node[v]['label']) for v in QG]
 #   nx.draw_networkx(QG)
 #   plt.show()
