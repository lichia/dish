dish
====

.. highlight:: bash

dish (**di**\ stributed **sh**\ ell) is an application agnostic
library for distributing work across resource scheduler clusters in a
relatively fault-tolerant way, aimed primarily at the use case of
running external programs from the shell, sometimes with a little glue
code to string them together. It was developed with bioinformatics
pipelines in mind, but is a general purpose tool that should be useful
whenever you have a lot of command line tools to run that need to feed
data to each other and the filesystem. As a small motivating example,
imagine you were doing some RNA-seq analysis and you wrote the
following bash script to analyze a single set of paired end reads::

  #!/bin/bash
  fastq1=$1
  fastq2=$2

  tophat -p 8 -o tophat_out /path/to/bowtie_index $fastq1 $fastq2
  cufflinks -p 8 -o cufflinks_out tophat_out/accepted_hits.bam

.. highlight:: python

This would work fine for analyzing a small amount of data, but what if
you wanted to use a resource scheduling system like TORQUE or SGE to
analyze larger amounts of data in parallel on a cluster? You might
have to write code to generate PBS scripts or similar, which would be
finicky, error prone, and not fault tolerant. With dish, you could
write something like::

  import os
  from dish.pipeline import Pipeline

  jobs = [{"description":"one",
            "fastq1":"/path/to/one_1.fastq",
            "fastq2":"/path/to/one_2.fastq"},
          {"description":"two",
           "fastq1":"/path/to/two_1.fastq",
           "fastq2":"/path/to/two_2.fastq"}]
  p = Pipeline(os.getcwd(), jobs, 20, "torque", "batch")
  p.run("tophat -p 8 -o tophat_out /path/to/bowtie_index {fastq1} {fastq2}",
        cores=8)
  p.run("cufflinks -p 8 -o cufflinks_out tophat_out/accepted_hits.bam",
        cores=8)

to run the same analysis distributed across a TORQUE cluster on the
queue ``batch`` using at most 20 cores. In this example we just
analyze two sets of paired-end reads, ``one`` and ``two``, but the way
you generate the list of ``jobs`` could be arbitrary complicated and
contain as many samples as you want.

Read on for more information about how it works.

The dish programming model
--------------------------
