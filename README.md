# MapReduce--sort-temperature-by-sampling

Sort the data set on temperature using map/reduce with sampling. 

The number of records may be disproportionate in each range of temperature. Hence, the response time is dictated by the mapper which has the largest number of records. This can be overcome by sampling input records to determine non-uniform temperature ranges that will distribute the number of records in each range approximately equally
