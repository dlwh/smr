SMR: Scala Map Reduce, redux

This is not the old SMR that was a wrapper for hadoop. 

*Why should I use this?*

You probably shouldn't.  You should probably use mesos/spark or scala-hadoop, or something.

*Then why are you making this?*

Because I don't have control over my cluster, so I can't run Spark.
Hadoop is really bad for machine learning workflows, and it's a
pain to get running if you don't have control over your cluster.

I also (mostly) don't need to worry about fault tolerance or such.
If that makes you not want to use this, so much the better. I'll add
it in as I need it.

SMR is designed to be easy to set up (for me!), easy to integrate
into your code, and fun to work with. It's going to work like the
scala collections framework, just distributed.

*License?*

Apache.
