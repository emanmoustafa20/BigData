# First Task

## Description

The objective of this task is to develop a Java-based MapReduce job that processes temperature data for various cities. The job will read input containing city names along with their respective temperatures in Fahrenheit. The primary function of this job is to calculate and output the average temperature for each city, with the temperature values being converted from Fahrenheit to Celsius. The final output should be a file listing each city accompanied by its average temperature in Celsius, demonstrating the effective use of MapReduce paradigms for data processing and aggregation.

 
Execution:
 The Job was executed on "twkdocker/nuf23461-hadoop" Docker Image

INPUT:
citiestemp.txt

Execution Command:
Hadoop jar task_one.jar /hdfs_path/citiestemp.txt /outputpath

OUTPUT:
output_file
