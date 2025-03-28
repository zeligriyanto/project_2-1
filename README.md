Zelig Riyanto, Reif Birzin, Olivia McCarty
## Getting started
Head to [Project 1](https://github.com/CSCI3390Spring2025/project_1) if you're looking for information on Git, template repositories, or setting up your local/remote environments.

## Resilient distributed datasets in Spark
This project will familiarize you with RDD manipulations by implementing some of the sketching algorithms the course has covered thus far.  

You have been provided with the program's skeleton, which consists of 5 functionss for computing either F0 or F2: the BJKST, tidemark, tug-of-war, exact F0, and exact F2 algorithms. The tidemark and exact F0 functions are given for your reference.

## Relevant data

You can find the TAR file containing `2014to2017.csv` [here](https://drive.google.com/file/d/1MtCimcVKN6JrK2sLy4GbjeS7E2a-UMA0/view?usp=sharing). Download and expand the TAR file for local processing. For processing in the cloud, refer to the steps for creating a storage bucket in [Project 1](https://github.com/CSCI3390Spring2025/project_1) and upload `2014to2017.csv`.

`2014to2017.csv` contains the records of parking tickets issued in New York City from 2014 to 2017. You'll see that the data has been cleaned so that only the license plate information remains. Keep in mind that a single car can receive multiple tickets within that period and therefore appear in multiple records.  

**Hint**: while implementing the functions, it may be helpful to copy 100 records or so to a new file and use that file for faster testing.  

## Calculating and reporting your findings
You'll be submitting a report along with your code that provides commentary on the tasks below.  

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
