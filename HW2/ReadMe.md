#  Big Data Computing: Homework 2
In this homework, you will run a Spark program on the CloudVeneto cluster. As for Homework 1, the objective is to estimate (approximately or exactly) the number of triangles in an undirected graph G=(V,E). 
More specifically, your program must implement two algorithms:

## ALGORITHM 1. 

The same as Algorithm 1 in Homework 1, so you must recycle method/function MR_ApproxTCwithNodeColors devised for Homework 1, fixing any bugs that we have pointed out to you.

## ALGORITHM 2. 
A 2-round MapReduce algorithm which returns the exact number of triangles. The algorithm is based on node colors (as Algorithm 1) and works as follows. Let $C \geq 1$ be the number of colors 
and let $h_C(⋅)$ be the hash function that assigns a color to each node used in Algorithm 1.

+ Round 1: For each edge $(u,v) \in E$  separately create $C$ key-value pairs $(ki,(u,v))$ with $i=0,1,…C−1$ where each key $k_i$ is a triplet containing the three colors $h_C(u), hC_(v), i$ sorted in non-decreasing order.
For each key $k=(x,y,z)$ let $L_k$ be the list of values (i.e., edges) of intermediate pairs with key $k$, compute the number $t_k$ of triangles formed by the edges of $L_k$  whose node colors, in sorted order, are $x, y, z$. 
Note that the edges of $L_k$ may form also triangles whose node colors are not the correct ones: e.g., $(x,y,y)$ with $y \neq z$.
An example of Round 1 is given in the following picture.
![image](https://github.com/Ska-p/Big-Data-Computing/assets/102731992/92b10c06-2bd4-4187-a237-a8a8fe93d7ff)

+ Round 2: Compute and output the sum of all $t_k$'s determined in Round 1. It is easy to see that every triangle in the graph G is counted exactly once in the sum. You can assume that the total number of $t_k$'s is small
, so that they can be garthered in a local structure. Alternatively, you can use some ready-made reduce method to do the sum.
