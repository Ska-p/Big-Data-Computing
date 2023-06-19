#  Big Data Computing: Homework 1

## TRIANGLE COUNTING. 
In the homework you must implement and test in Spark two MapReduce algorithms to count the number of distinct triangles in an undirected graph $G=(V,E)$, where a triangle is defined by 3 vertices $u,v,w$
in $V$, such that $(u,v),(v,w),(w,u)$ are in $E$. Triangle counting is a popular primitive in social network analysis, where it is used to detect communities and measure the cohesiveness of those communities. 
It has also been used is different other scenarios, for instance: detecting web spam (the distributions of local triangle frequency of spam hosts significantly differ from those of the non-spam hosts), 
uncovering the hidden thematic structure in the World Wide Web (connected regions of the web which are dense in triangles represents a common topic), 
query plan optimization in databases (triangle counting can be used for estimating the size of some joins).

Both algorithms use an integer parameter $C \geq 1$, which is used to partition the data.

### ALGORITHM 1: 
Define a hash function $h_C$ which maps each vertex u in V into a color $h_C(u)$ in $[0,C−1]$. To this purpose, we advise you to use the hash function $h_C(u)=((a⋅u+b)modp)modC$
where p=8191 (which is prime), a is a random integer in $[1,p−1]$, and b is a random integer in $[0,p−1]$.

Round 1:
Create $C$ subsets of edges, where, for $0 \leq i < C$, the i-th subset, $E(i)$ consist of all edges $(u,v)$ of E such that $h_C(u)=h_C(v)=i$. 
Note that if the two endpoints of an edge have different colors, the edge does not belong to any $E(i)$ and will be ignored by the algorithm.
Compute the number $t(i)$ triangles formed by edges of $E(i)$, separately for each $0 \leq i < C$. 
Round 2: 
Compute and return $tfinal = C^2 \sum_{0 \leq i < C} t(i)$ as final estimate of the number of triangles in $G$.

Develop an implementation of this algorithm as a method/function MR_ApproxTCwithNodeColors.

### ALGORITHM 2:
Round 1:
Partition the edges at random into $C$ subsets $E(0),E(1),...E(C−1)$. Note that, unlike the previous algorithm, now every edge ends up in some $E(i)$.
Compute the number $t(i)$ of triangles formed by edges of $E(i)$, separately for each $0 \leq i < C$.
Round 2: 
Compute and return  $tfinal = C^2 \sum_{0 \leq i < C} t(i)$ as final estimate of the number of triangles in $G$.
