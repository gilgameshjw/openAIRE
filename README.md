# openAIRE

scrape and write/analyse data with spark

# Code

I have decided to decompose the code into the following bits.
The library dependencies should be quite clear, I am using pyarrow. I am not 

* utils.py 
	* utilities to massage the XML data via brute force
* get-data-from-unibielefeld.py 
	* collecting the data from the repository base
	* gathering and massaging the data into a dataframe 
	* save the dataframe under /data/df.csv (one has to run get-data-....py to generate the full file).
	* > python3 get-data-from-unibielefeld.py > load-output.log
* pyspark-script.py
	* this it the script using pyspark
	* > time pyspark < pyspark-script.py > spark-output.log
*  output.txt
	* my script output
* requirements	
	* as generated with pip freeze. My main lib unfiltered, even though I am using conda usually...