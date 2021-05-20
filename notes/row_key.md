# Row Key Storage and Design

The HBase data is stored in different region servers depend on the row key. And row keys are sorted in alphabet order, 
so if we just connect the IDs and use them as the row key, all the data will be put into a single region server of HBase,
and this is a hotspot issue. To avoid it, we usually use: hash(ID) to make the data distribute evenly across all region
servers.  

For some use cases, we also want to select all the data with the same IDs to be put together, this will be a tradeoff.
 