SparkXpehh
===========
SparkXpehh is a tool for efficiently calculating Extended Haplotype Homozygosity (EHH), the Integrated Haplotype Score (iHS) and the Cross Population Extended Haplotype Homozogysity (XP-EHH) statistic.

Install
--------

SparkXpehh is build using Apache Maven. To build SparkXpehh, run:

        git clone https://github.com/zhouweiyg/SparkXpehh.git
        cd SparkXpehh
        mvn compile package

You will get a jar file in target folder if you package the source file successfully. Then, you can run SparkXpehh.

Usage
--------

Usage:   SparkXpehh [options]

| Parameter name | Parameter type | Parameter meaning |
| ----- | :---- | :----- |
| --h |string	| ped file path |
| --m | 	string |	map file path |
| --p |	string |	pop file path |
| --r | 	string |	result file path |
| --s |	int | 	stepsize(default 16) |
| --t |	float |	threshold(default 0.05) |
| --ps | 	int |	partition size(default 138) |
| --a |	int |	accuracy  size(default 10) |


The default output format of SparkXpehh recruitment result file looks like:

        ReadNumber	ReadLength	E-value	AlignmentLength	Begin	End	Strand	Identity	Begin	End  ReferenceSequenceName

        1	75nt	4.7e-25	69	1	-	95.65%	3450573	3450641	Ruminococcus_5_1_39B_FAA
        9	75nt	1.2e-25	1	64	+	98.44%	1029618	1029681	Alistipes_putredinis_DSM_17216
        10	75nt	2.5e-23	1	72	+	93.06%	3128442	3128513	Prevotella_copri_DSM_18205
        11	75nt	9.6e-23	75	2	-	91.89%	1018573	1018646	Prevotella_copri_DSM_18205
        14	75nt	1.0e-07	4	45	+	90.48%	301211	301252	Bacteroides_capillosus_ATCC_29799
        17	75nt	1.6e-28	69	1	-	98.55%	133030	133098	Bacteroides_vulgatus_ATCC_8482
        17	75nt	1.6e-28	69	1	-	98.55%	1718708	1718776	Bacteroides_D4
        17	75nt	1.6e-28	69	1	-	98.55%	601790	601858	Bacteroides_9_1_42FAA


Run SparkXpehh:
--------
SparkXpehh is based on Spark platform and it's data is stored in Hadoop HDFS, you should upload the reads and reference file to the HDFS cluster before you run the SparkXpehh programming.  

        spark-submit --class com.ynu.SparkXpehh --master spark://{spark master address}:{port} --name {app name} {SparkXpehh jar file} --read {read file path on HDFS} --ref {reference file path on HDFS} --result {result store path}  --identity 90 --aligment 40

SparkXpehh also provide a function to create reference index and store it to HDFS, so you can save a lots of time if you run the test with the same reference file. 

        spark-submit --class com.ynu.CreateRefIndexToHDFS --master spark://{spark master address}:{port} --name {app name} {SparkXpehh jar file} --ref {reference file path on HDFS} --kmersize 11
        
After you create the reference index, you can use it in the new test.

        spark-submit --class com.ynu.SparkXpehh --master spark://{spark master address}:{port} --name {app name} {SparkXpehh jar file} --read {read file path on HDFS} --ref {reference file path on HDFS} --refindex {reference index file path on HDFS} --result {result store path}  --identity 90 --aligment 40

