PostgreSQL index based on Adaptive Radix Tree
===

Experimental, research driven implementation of ART as PostgreSQL index extension. Opposite to default ART main memory implementation, 
this one is implemented to be used in PG so focused to be disk persistent.

ART algorithm is influenced by awesome libart library (https://github.com/armon/libart)

Index support is limited with only INSERT/SCAN functionality.
