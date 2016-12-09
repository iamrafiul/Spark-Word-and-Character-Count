# Spark Word and Character Count
This is a simple python script for counting words and characters from a text file in Spark.

## Install Spark
I have used Spark from the Cloudera Quickstart VM for VirtualBox which you can download from [here](http://www.cloudera.com/downloads/quickstart_vms/5-8.html). Download and import the .ovf file in VirtualBox and once the setup is done, you can use Spark from the terminal using the following command.

```
pyspark
```

## Project Settings
I have kept the input text file(textfile.txt) at HDFS in the directory /user/root/. 

To keep your file in HDFS, you can do the following.

If you do not have that "root" directory, you can create it using the following command
```
sudo -u hdfs hadoop fs -mkdir /user/root
```
Then copy your local file in the HDFS directory. For example if you have a file "Test.txt" in Documents directory, you can copy it to HDFS using the following command.
```
hadoop fs -copyFromLocal Documents/Test.txt hdfs://localhost/user/root/Test.txt
```
Once you are done with copying the file to HDFS, you can run the script "word_count.py" from it's directory. Mind it, it's not a normal python script to execute so "python word_count.py" will not work so we have to use spark commands to do that. 

Run the script using the following command:
```
spark-submit --master yarn --deploy-mode client --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" word_count.py
```

If everything goes well, you will get 3(Three) files "output.txt"(for the word and character counts in text format), "output.csv"(for the word counts in CSV format) and "char_output.csv"(for the character counts in CSV format) in the same directory where "word_count.py" is.

Voila!! 
