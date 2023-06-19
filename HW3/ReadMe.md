# Big Data Computing: Homework 3
In this homework, you will use the Spark Streaming API to devise a program which processes a stream of items and assesses experimentally the space-accuracy tradeoffs featured by the count sketch 
to estimate the individual frequencies of the items and the second moment F2.

## Spark streaming setting used
For the homework, we created a server which generates a continuous stream of integer items. The server has been already activated on the machine algo.dei.unipd.it and emits the items as strings on port 8888. 
Your program will define a Spark Streaming context that accesses the stream through the method socketTextStream which transforms the input stream (coming from the specified machine and port number) 
into DStream (Discretized Stream) of batches of items arrived during a time interval whose duration is specified at the creation of the context. 
A method foreachRDD is then invoked to process the batches one after the other. Each batch is seen as an RDD and a set of RDD methods are available to process it. 
Typically, the processing of a batch entails the update of some data structures stored in the driver's local space (i.e., its working memory) which are needed to perform the required analysis. 
The beginning/end of the stream processing will be set by invoking start/stop methods from the context sc. For the homework, the stop command will be invoked after (approximately) 10M items have been read. 
The threshold 10M will be hardcoded as a constant in the program.

## TASK for HW3.
You must write a program which receives in input the following 6 command-line arguments (in the given order):

An integer $D$: the number of rows of the count sketch
An integer $W$: the number of columns of the count sketch
An integer $left$: the left endpoint of the interval of interest
An integer $right$: the right endpoint of the interval of interest
An integer $K$: the number of top frequent items of interest
An integer $portExp$: the port number
The program must read the first (approximately) 10M items of the stream $\Sigma$ generated from machine **algo.dei.unipd.it** at port $portExp$ and compute the following statistics. 

Let $R$ denote the interval $\[left,right\]$ and let $\Sigma_R$ be the substream consisting of all items of $\Sigma$ belonging to $R$. The program must compute: 
+ $D \times W$ count sketch for $\Sigma_R$. To this purpose, you can use the same family of hash functions used in Homeworks 1 and 2, namely $((ax+b) mod p) mod C$, where $p=8191$, $a$ is a random integer in $\[1,p-1\]$ and $b$ is a random integer in $\[0,p-1\]$. 
The value $C$ depends on the range you want for the result.
+ The exact frequencies of all distinct items of $\Sigma_R$
+ The true second moment $F2$ of $\Sigma_R$. To avoid large numbers, normalize $F2$ by dividing it by $|\Sigma_R|^2$.
+ The approximate second moment $\tilde{F}2 of $\Sigma_R$ using count sketch, also normalized by dividing it by $|\Sigma_R|^2$.
+ The average relative error of the frequency estimates provided by the count sketch where the average is computed over the items of $u \in $\Sigma_R$ whose true frequency is $f_u \geq \phi(K)$, where $\phi(K)$ is the K-th
largest frequency of the items of $\Sigma_R$ Recall that if $\tilde{f}_u$ is the estimated frequency for $u$, the relative error of is $\frac{|f_u−\tilde{f}_u|}{f_u}$.
