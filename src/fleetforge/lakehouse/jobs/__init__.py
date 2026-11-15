"""Spark jobs of the lakehouse. They need pyspark and Java (the ``lakehouse`` extra and
the image in infrastructure/lakehouse/spark), so pyspark is imported inside
functions: the pure modules stay importable without it.

Status: written against the pure reference modules; NOT yet executed. They run for
the first time in the deferred lakehouse tests (docs/architecture/milestone-3.md,
"Deferred execution").
"""
