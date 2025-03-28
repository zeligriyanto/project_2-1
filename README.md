Zelig Riyanto, Reif Birzin, Olivia McCarty

1. **(3 points)** 
ExactF2
Local Results: Time elapsed:64s. Estimate: 8567966130
GCP Results: Time elapsed: 241s. Estimate: 8567966130
2. **(3 points)** 
Tug-of-War F2 Approximation
Local Results: Tug-of-War F2 Approximation. Width :10. Depth: 3. Time elapsed:23s. Estimate: 9185400028
GCP Results:  Tug-of-War F2 Approximation. Width :10. Depth: 3. Time elapsed:93s. Estimate: 8530379119
3. **(3 points)** 
BJKST
Local Results: Bucket Size: 25, Trials: 5, Time elapsed:402s. Estimate: 6553600

GCP Results: Bucket Size: 25, Trials: 5, Time elapsed:172s, Estimate: 6291456
4. **(1 point)** Compare the BJKST algorithm to the exact F0 algorithm and the tug-of-war algorithm to the exact F2 algorithm. Summarize your findings.

BJKST: Bucket Size: 25, Trials: 5, Time elapsed:172s, Estimate: 6291456
F0: Time Elapsed:196s Estimate: 7406649

The BJKST algorithm provides a faster, but more approximate estimate of the number of distinct element. Given the small bucket size and limited trials, it is underestimated by around 15%. BJKST offers speed and lower memory use at the cost of some accuracy.

F2 Results: Time elapsed: 241s. Estimate: 8567966130
Tug-Of-War Results: Time elapsed:93s. Estimate: 8530379119

The tug-of-war is significantly faster than the F2 approximation, so it has much lower computational cost. The estimates only have a 0.44% relative error.
