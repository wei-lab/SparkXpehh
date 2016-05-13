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

        5386	0.00982016	0.01314943	-0.29194100
        9724	0.01281606	0.01602563	-0.22349024
        9728	0.02265941	0.02710481	-0.17913598
        11184	0.02399037	0.03035566	-0.23533049
        12540	0.02483094	0.03213575	-0.25787866
        12544	0.02458190	0.03071724	-0.22281366
        12554	0.03668928	0.04580508	-0.22191039



Run SparkXpehh:
--------
SparkXpehh is based on Spark platform and it's data is stored in Hadoop HDFS, you should upload the files to the HDFS cluster before you run the SparkXpehh programming.  

        spark-submit --class com.ynu.xpehh.XpehhMemory --master spark://{Spark Master Address}:{port} –name {Application Name} {SparkXPehh Jar file path } -h {ped file path } -m {map file path} -p {pop file path} -r {Result file path} -s {stepsize(default 16)} -t {threshold(default 0.05)} -ps {partition size(default 138)} -a {accuracy  size(default 10)}



