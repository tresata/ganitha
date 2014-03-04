# Ganitha

Tresata is proud to release *Ganitha*, our first open-source library. Ganitha (derived from the Sanskrit word for mathematics, or science of computation) is a [Scalding](https://github.com/twitter/scalding) library with a focus on machine-learning and statistical analysis.

The first two pieces to be open-sourced are our integration of [Mahout](https://github.com/apache/mahout) vectors into Scalding, and our clustering (K-Means) implementation. We plan to add more later.

## Ganitha-Mahout

To make mahout vectors usable in Scala/Scalding we did the following:

* **Pimp Mahout vectors**: We used the [pimp-my-library](http://www.artima.com/weblogs/viewpost.jsp?thread=179766) pattern in Scala to make Mahout vectors more friendly to use (see `RichVector.scala`). Note that we decided to not implement `RichVector` as an `IndexedSeq`, but rather as an `Iterable`, for our first iteration. We also didn't implement `IterableLike` (or `IndexedSeqLike`) and `CanBuildFrom`, which would allow operations like `vector.take(3)` and `vector.map(f)` to return new Mahout vectors. The reason for this was that we were not happy with the interaction between the builders and the pimp-my-library pattern. We might still add these features in the future. As an alternative, we provided the `vectorMap` methods that return Mahout vectors. Thanks to `RichVector`, you can now do things like:

  ```
  scala> import com.tresata.ganitha.mahout._
  scala> import com.tresata.ganitha.mahout.Implicits._
  scala> // create a sparse vector of size 6 with elements 1, 3 and 5 non-zero
  scala> val v = RichVector(6, List((1, 1.0), (3, 2.0), (5, 3.0)))
  v: org.apache.mahout.math.Vector = {5:3.0,3:2.0,1:1.0}
  ```

  (We can see it's using a Mahout `RandomAccessSparseVector` with `v.getClass`). Likewise, we can create a dense vector with 3 elements as follows:

  ```
  scala> val v1 = RichVector(Array(1.0,2.0,3.0))
  v1: org.apache.mahout.math.Vector = {0:1.0,1:2.0,2:3.0}
  ```

  We can also perform basic math and vector operations (map, fold, etc.) on the vectors. Elements inside the vectors can be accessed and set (since Mahout vectors are mutable), however, this is not encouraged.

  ```
  scala> (v + 2) / 2
  res1: org.apache.mahout.math.Vector = {5:2.5,4:1.0,3:2.0,2:1.0,1:1.5,0:1.0}
  scala> v.map(x => x * 2).sum
  res2: Double = 12.0
  scala> v.fold(0.0)(_ + _)
  res3: Double = 6.0
  scala> v(3)
  res4: Double = 2.0
  scala> v(3) = 3.0
  scala> v
  res5: org.apache.mahout.math.Vector = {5:3.0,3:3.0,1:1.0}
  scala> v * v
  res6: org.apache.mahout.math.Vector = {5:9.0,3:4.0,1:1.0}
  ```

  The `nonZero` method provides access to the non-zero elements as a scala `Iterable`.

  ```
  scala> v.nonZero.toMap
  res7: scala.collection.immutable.Map[Int,Double] = Map(5 -> 3.0, 3 -> 3.0, 1 -> 1.0)
  ```

  Dense vectors can be converted to sparse, and vice versa.

  ```
  scala> v1.toSparse.getClass
  res8: java.lang.Class[_ <: org.apache.mahout.math.Vector]
          = class org.apache.mahout.math.RandomAccessSparseVector
  ```

  The `vectorMap` operation provides access to the assignment operation on a Mahout vector, but as a non-mutating operation (it creates a copy first).

  ```
  scala> v.vectorMap(x => x * 2)
  res9: org.apache.mahout.math.Vector = {5:6.0,3:4.0,1:2.0}
  ```



* **Make serialization transparent**: Mahout's vectors come with a separate class called `VectorWritable` that implements `Writable` for serialization within Hadoop. The issue with this is that you cannot just register `VectorWritable` as a Hadoop serializer and be done with it. If you did this then you would have to constantly wrap your Mahout vectors in a `VectorWritable` to make them serializable. To make the serialization transparent we added `VectorSerializer`, a [Kryo](http://code.google.com/p/kryo/) serializer that defers to `VectorWritable` for the actual work. All one has to do is register `VectorSerializer` with Kryo, and serialization works in Scalding. For example, if you are using a `JobConf` you can write:

  ```scala
  VectorSerializer.register(job)
  ```

  The same applies to a Scalding `Config` (which is a `Map[AnyRef, AnyRef]`):
  ```scala
  VectorSerializer.register(config)
  ```

## K-Means clustering

[K-means clustering](http://en.wikipedia.org/wiki/K-means_clustering) consists of partitioning data points into k 'clusters' where each point belongs to the cluster with the nearest mean. The process of refining the centers of the clusters is commonly known as [Lloyd's algorithm](http://en.wikipedia.org/wiki/Lloyd%27s_algorithm), however there exist heuristic algorithms to seed the initial selection of cluster centers in order to improve the rate of convergence of Lloyd's algorithm. [K-Means++](http://en.wikipedia.org/wiki/K-means%2B%2B) offers an improvement over random initial selection, and more recently, K-Means|| offers an initialization technique that greatly cuts down on the number of iterations needed to determine initial clusters, a very desirable optimization in Hadoop applications, where significant overhead is involved in each iteration.

Ganitha provides an extensible interface for handling vector operations using different representations for data points, including Mahout vectors (which can contain categorical and textual features in addition to numerical). The `VectorHelper` trait can be used to specify how vectors are defined from the input and how distances are calculated between vectors.

K-Means in Ganitha (currently) reads vectors from [Cascading](http://www.cascading.org/) Sequence files, and the algorithm writes a list of vectorid-clusterid pairs to a Tap, as well as a list of cluster ids with coordinates.

### References

K-Means:
Lloyd, S., "Least squares quantization in PCM". *IEEE Trans. Information Theory*, 28(2):129-137, 1982.

K-Means++:
Arthur, D. and Vassilvitskii, S. (2007). "k-means++: the advantages of careful seeding".
*Proc. ACM-SIAM Symp. Discrete Algorithms*. pp. 1027–1035.

K-Means||:
Bahmani, B. et al. (2012). "Scalable k-means++". *Proceedings of the VLDB Endowment*, 5(7), 622-633.

## Getting started with Ganitha and K-Means

Ganitha uses [sbt](http://www.scala-sbt.org/) for generating builds. To create a runnable jar distribution, run ```sbt update``` and ```sbt assembly```. Unit tests are included and can be run using ```sbt test```.

To run K-Means clustering on a test set of data, stored as a comma-separated values file with a header (in this example, with a file on Hadoop named *100kPoints.csv* with the header (```id,x,y```), run the following command from within the ganitha directory:

```
hadoop jar ganitha-ml/target/scala-2.9.2/ganitha-ml-assembly-0.1-SNAPSHOT.jar com.twitter.scalding.Tool com.tresata.ganitha.ml.clustering.KMeansJob --hdfs --vecType StrDblMapVector --distFn euclidean --k 100 --id id --features x y --input 100kPoints.csv --vectors 100kVectors --vectorOutput vectorAssignments --clusterOutput centroids
```

This will use the `id` columns as the vector id, and will encode the coordinates(`x` and `y`) as ```Map[String, Double]``` vectors (using the ```StrDblMapVector``` VectorHelper), under a Euclidean space, and run the algorithm on *k*=100 clusters. The output is written to a ```vectorAssignments``` file on Hadoop, with the cluster centroids written to ```centroids```. The `vectors` argument specifies a location for the Cascading Sequence file that serves as the input for ```KMeans```.

## License

Copyright 2014 Tresata, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
