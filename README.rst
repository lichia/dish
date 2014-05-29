dish
====

dish (**di**\ stributed **sh**\ ell) is an application agnostic
library for distributing work across resource scheduler clusters in a
relatively fault-tolerant way, aimed primarily at the use case of
running external programs, sometimes with a little glue code to string
them together. It was developed with bioinformatics pipelines in mind,
but is a general purpose tool that should be useful whenever you have
a lot of command line tools to run that need to feed data to each
other and the filesystem. As a small motivating example, imagine you
were doing some RNA-seq analysis and you wrote the following bash
script to analyze a single set of paired end reads

.. code-block:: bash

  #!/bin/bash
  fastq1=$1
  fastq2=$2

  tophat -p 8 -o tophat_out /path/to/bowtie_index $fastq1 $fastq2
  cufflinks -p 8 -o cufflinks_out tophat_out/accepted_hits.bam

This would work fine for analyzing a small amount of data, but what if
you wanted to use a resource scheduling system like TORQUE or SGE to
analyze larger amounts of data in parallel on a cluster? You might
have to write code to generate PBS scripts or similar, which would be
finicky, error prone, and not fault tolerant. With dish, you could
write something like

.. code-block:: python

  import os
  from dish.pipeline import Pipeline

  jobs = [{"description":"one",
            "fastq1":"/path/to/one_1.fastq",
            "fastq2":"/path/to/one_2.fastq"},
          {"description":"two",
           "fastq1":"/path/to/two_1.fastq",
           "fastq2":"/path/to/two_2.fastq"}]
  p = Pipeline(os.getcwd(), jobs, 20, "torque", "batch")
  p.start()
  p.run("tophat -p 8 -o tophat_out /path/to/bowtie_index {fastq1} {fastq2}",
        cores=8)
  p.run("cufflinks -p 8 -o cufflinks_out tophat_out/accepted_hits.bam",
        cores=8)
  p.stop()

to run the same analysis distributed across a TORQUE cluster on the
queue ``batch`` using at most 20 cores. In this example we just
analyze two sets of paired-end reads, ``one`` and ``two``, but the way
you generate the list of ``jobs`` could be arbitrary complicated and
contain as many samples as you want.

Of course, dish is not really a shell at all; it is a Python library;
the name was too good to pass up. The name is somewhat justified,
however, in that dish deals with a similar problem to an actual shell
(managing the running of other programs).

Read on for more information about how it works.

The dish programming model
--------------------------

dish is designed for embarassingly parallel problems—you have a bunch
of data chunked into basically similar pieces (e.g. many sets of
paired-end reads, many similar images, etc.), and you want to do
essentially the same series of steps to each piece. The abstraction
that dish uses to represent this entire process is a ``Pipeline``
object.  A Pipeline is constructed with a list of ``jobs``, each of
which represents one "chunk" of your data. You can then call methods
on the pipeline that cause something to happen to each job. dish
handles distributing the work across your cluster and making it
fault-tolerant. Under the hood this is achieved using
ipython-cluster-helper_, so dish should support all the resource
schedulers that it does.

Constructing a simple Pipeline might look like:

.. code-block:: python

   from dish.pipeline import Pipeline

   jobs = [{"description":"test1"},{"description":"test2"}]

   p = Pipeline("/nfs/workdir", jobs, 20, "torque", "batch")

The signature of the ``Pipeline`` constructor is::

  Pipeline(workdir, jobs, cores, system, queue)

So the above invocation will make a ``Pipeline`` that produces output
in ``/nfs/workdir``, submitting jobs to a TORQUE scheduler on the
queue ``batch`` and using a maximum of 20 cores. Note that for now the
``workdir`` must be readable and writeable over some sort of network
filesytem from all work nodes of the resource scheduler.

The ``jobs`` argument must be a list of ``dict``, an each dict must
have a ``description`` key, but other than that can be whatever you
want. Typically each job will initially hold information about the
location of it's input data (paths to files, urls, etc.) and be
modified as the pipeline runs to contain intermediate processing
information. The ``description`` key is required because it is used to
create subdirectories in the ``workdir`` in which to put the output of
each job.

After constructing a ``Pipeline``, we can call it's ``start`` method
to initialize it. This makes the aforementioned working directories
for each job, as well as doing miscellanious tasks like setting up
logging. So after constructing the above Pipeline we could call::

  p.start()

and then do an ``ls`` on ``/nfs/workdir`` and see two new directories,
``test1`` and ``test2``. We can also examine ``p.jobs`` and see that
each job now has a new key, ``workdir``, whose value is the absolute
path to the jobs working directory::

  >>> p.jobs
  [{'description': 'test1', 'workdir': '/nfs/workdir/test1'},
   {'description': 'test2', 'workdir': '/nfs/workdir/test2'}]

Now we can call a variety of methods on ``p`` to cause work to be done
on each job.

The most useful of these is probably ``p.run``, which runs an external
shell command once per job in the pipeline. For example::

  p.run("touch {workdir}/example")

will produce an empty file, ``example`` in the work directory of each
job. This shows a useful fact about the ``run`` method, which is that
the string passed to it is formatted with the contents of the ``job``
before being run. It's also worth mentioning that by default commmands
passed to ``run`` are executed in the working directory of each job,
so the above example could also be written with a relative path::

  p.run("touch example")

and do the same thing. This shows how we can put together data processing
pipelines with dish. Since we don't have any actual data in this trivial
example, let's make some up at random::

  p.run("base64 /dev/urandom | head -c 10000 > {workdir}/data")

Now each job's workdir has a file ``data`` with some random ASCII in
it. Now let's count the number of ``A`` characters in each file::

  p.run("grep -o A data | wc -l > count")

And now each job has a file, ``count``, containing the count.

Admitedly this is a silly example, but it illustrates the basics of
how to use dish. In practice you would probably start with a ``data``
key on each job whose value is the appropriate filepath and then do
something like::

  p.run("grep -o A {data} | wc -l > count")

This hopefully gets across what programming with dish feels like, now
let's dive into some more advanced features.

Resource Scheduling
~~~~~~~~~~~~~~~~~~~

dish is capable of using the pipeline's underlying resource scheduler
to make intelligent use of cluster resources. The ``run`` method has
three optional keyword arguments, ``cores``, ``mem``, and ``max`` for
specifying the resources a command requires. For example, let's say
you want to run a command like ``tophat``, which can make use of
multiple cores to improve performance. Maybe you also know that for
the work you're doing, tophat will require at least 12 GB of
memory. You could write:

..  code-block:: python

    p.run("tophat -p 8 -o tophat_out /path/to/bowtie_index {fastq1} {fastq2}",
          cores=8,
          mem=12)

(this of course assumes you have ``{fastq1}`` and ``{fastq2}`` keys on
each job which contain the paths to the appropriate files.)

This will cause dish to only run as many commands in parallel as are
feasible for the given constraints. So if you constructed ``p`` to use
20 cores, at most 2 ``tophat``s will be run in parallel; if ``p`` was
constructed with 80 cores, 10 will be run in parallel, etc. dish will
also tell the underlying scheduling system about your restrictions so
it doesn't overcommit cores or memory on any given machine.

In addition to ``cores`` and ``mem``, run also takes an optional
``max`` keyword argument, which is interpreted as a hard limit on the
number of commands to run in parallel, regardless of how many cores
are available. This is useful if, for example, you know that a given
command will stress some sort of storage backend and that if more than
a certain number are run at once, failiures will occur.


Storing command output
~~~~~~~~~~~~~~~~~~~~~~

It's often useful to use the ``job`` dictionary as a place to hold
small amounts of information pertaining to the state of a running
job. You can store the output of a run command on the ``job`` using
the ``capture_in`` keyword argument. For example::

  p.run("base64 /dev/urandom | head -c 10", capture_in="random_data")

will get 10000 bytes of random data from /dev/urandom for each job and
store the result in the jobs ``"random_data"`` key.

We could then do something like::

  p.run("touch {random_data}")

to create a randomly named file in each job's workdir.


Running python code
~~~~~~~~~~~~~~~~~~~

While dish is aimed primarily at running external tools, it is
sometimes useful or necessary to write some glue code between them,
e.g. to parse the output of some program and munge it into a format
that can be input to another. dish makes this relatively painless with
the ``map`` method.

``p.map`` takes a single argument, a function whose signature is
``f(job, logger)``. This function will be run in parallel and called
once for each job, being passed the ``job`` and dish's logger, on
which you can call all the standard methods (``info``, ``warning``,
``error``, etc.) and have the results logged both to a job specific
logfile and a logfile for the entire pipeline. ``f`` should modify the
job in place and not return anything. For example::

  def capitalize_descrpition(job, logger):
      job["capitalized_description"] = job["description"].upper()
  p.map(capitalize_description)

Will result in each job getting a ``"capitalized_description"`` key::

  >>> p.jobs
  [{'capitalized_description': 'TEST1',
    'description': 'test1',
    'workdir': '/Users/james/scratch/workdir/test1'},
   {'capitalized_description': 'TEST2',
    'description': 'test2',
    'workdir': '/Users/james/scratch/workdir/test2'}]

We could then use this key in future operations, for example::

  p.run("echo WHY ARE WE YELLING > {capitalized_description}.txt")

``map`` takes the same resource scheduling keyword arguments as ``run`` and
they behave in the same way.

Note that calling ``p.map(f)`` will cause an IPython cluster to be
launched to distribute the work over many machines. If ``f`` is not
computationally intensive, the networking overhead of setting up a new
IPython cluster can dwarf the cost of the work to be done. For these
situations, there is another method ``localmap``, which has the same
interface as ``map``, but just runs locally as a thin wrapper around
Python's ``map`` builtin, avoiding networking overhead.



.. _ipython-cluster-helper:
   https://github.com/roryk/ipython-cluster-helper
