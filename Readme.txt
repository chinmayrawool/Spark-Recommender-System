-------------------------------------------------------------------------------------
User-User based Movie Recommender System
Name: Chinmay Rawool
Email id: crawool@uncc.edu

Report URL: http://webpages.uncc.edu/crawool/
-------------------------------------------------------------------------------------

For execution of User based Similarity program:(userSimilarity.py)

Step 1: Makee directory in hdfs
hadoop fs -mkdir /user/<username>/

Step 2:
Put the movies.csv, ratings.csv and userSimilarity.py in the hdfs with the following command.

	hadoop fs -put /users/<username>/userSimilarity.py /user/<username>/
	hadoop fs -put /users/<username>/movies.csv /user/<username>/
	hadoop fs -put /users/<username>/ratings.csv /user/<username>/

Step 3: 
Submit the code using the following command for finding the recommendation for user 1:
	Format: programFile ratingsFile moviesFile current_UserId > outputFile
	spark-submit userSimilarity.py ratings.csv movies.csv 1 > User1Top50Movies.txt

Step 4: Check the output in User1Top50Movies.txt using the vim command.
	eg: vi User1Top50Movies.txt

Step 5:
Put the first.py(ALS mmlib code) in the hdfs with the following command.
	hadoop fs -put /users/<username>/first.py /user/<username>/

Step 6: 
Submit the code using the following command for finding the performance metric:
	Format: programFile ratingsFile > outputFile
	spark-submit userSimilarity.py ratings.csv > als.txt

Step 7: Check the output in User1Top50Movies.txt using the vim command.
	eg: vi als.txt


--------------------------------------------------------------------------------------