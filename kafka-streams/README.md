Task1: Simple Transformations

Go to the "org.gft.big.data.practice.kafka.academy.streams.transformations.UserNamesTransformer" and implement the 
"transform" method so that it:

* filters out all users whose name does not begin with an "A"
* filter out all users below the age of 18
* transforms each User to his name and surname, separated by a space

Once this is done, run the "UserNamesTransformerTest" to verify the correctness of your implementation.


Task2: Aggregations

Go to the "org.gft.big.data.practice.kafka.academy.streams.aggregations.NameHistogramCalculator" and implement the 
"calculateNameHistograms" so that it groups all the Users by their names and for each name, it calculates the total
count of all the people having each of those names and returned a KTable formed in the process. 

Once this is done, run the "NameHistogramCalculatorTest" to verify the correctness of your implementation.


Task3: Cross Projections

Go to the "org.gft.big.data.practice.kafka.academy.streams.joins.UserMatcher" class and implement the "matchUsers" 
method so that from the two streams, it calculates all the pairs of users having identical surnames and returns them as 
a Stream<User, User>.

Once this is done, run the "UserMatcherTest" to verify the correctness of your implementation.