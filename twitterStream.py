from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    global pwords 
    pwords = load_wordlist("positive.txt")
    global nwords
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    positive_points = []
    negative_points = []
    x_points = []
    for lst in counts:
    	for tup in lst:
    		if tup[0] == ("positive"):
    			positive_points.append(tup[1])
    		else:
    			negative_points.append(tup[1])
    
    for i in range(len(positive_points)):
    	x_points.append(i)
    
    plt.xlabel('Time Step')
    plt.ylabel('Word Count')
    plt.axis([0, 12, 0, 300])
    plt.xticks([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    plt.plot(x_points, positive_points, '-bo', label = 'positive')
    plt.plot(x_points, negative_points, '-go', label = 'negative')
    plt.legend()
    plt.show()



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    file_handle = open(filename, 'rU')
    word_list = []
    for line in file_handle:
    	word_list.append(line.strip())
    file_handle.close()
    return word_list

def parse_word(word):
	if word in pwords:
	    return ("positive", 1)
	if word in nwords:
		return ("negative", 1)
	else:
		return ("general", 1)
		
def calculateCount(count, runningCount):
	if runningCount is None:
		runningCount = 0
	return sum(count, runningCount)
	
def stream(ssc, pwords, nwords, duration): 
	
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    
    words = tweets.flatMap(lambda tweet: tweet.split(" "))
    pairs = words.map(parse_word).filter(lambda x: "positive" in x or "negative" in x)
    wordCount = pairs.reduceByKey(lambda x, y: x + y) 
    runningCount = wordCount.updateStateByKey(calculateCount)
    runningCount.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    
    counts = []
    wordCount.foreachRDD(lambda rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
