# User-User Similarity computation on pySpark

import sys
from itertools import combinations
import numpy as np
import pdb

from pyspark import SparkContext


def parseOnMovieId(line):
    '''
    movie_id,(user_id,rating)
    '''
    line = line.split(",")
    return int(line[1]),(int(line[0]),float(line[2]))
	
def parseMovie(line):
    '''
    movie_id,movie_title
    '''
    line = line.split(",")
    return int(line[0]),line[1]
	
def parseOnUser(line):
    '''
    Parse each line of the specified data file, assuming a "," delimiter.
    Key is user_id, converts each rating to a float.
    '''
    line = line.split(",")
    return int(line[0]),(int(line[1]),float(line[2]))

def keyOnUserPair(movie_id,user_and_rating_pair):
    ''' 
    Convert each movie and co_rating user pairs to a new vector
    keyed on the user pair ids, with the co_ratings as their value. 
    '''
    (user1_with_rating,user2_with_rating) = user_and_rating_pair
    user1_id,user2_id = user1_with_rating[0],user2_with_rating[0]
    user1_rating,user2_rating = user1_with_rating[1],user2_with_rating[1]
    return (user1_id,user2_id),(user1_rating,user2_rating)

def calcSim(user_pair,rating_pairs):
    ''' 
    For each user-user pair, return the specified similarity measure,
    along with co_raters_count.
    '''
    sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
    
    for rating_pair in rating_pairs:
        sum_xx += np.float(rating_pair[0]) * np.float(rating_pair[0])
        sum_yy += np.float(rating_pair[1]) * np.float(rating_pair[1])
        sum_xy += np.float(rating_pair[0]) * np.float(rating_pair[1])
        n += 1

    cos_sim = cosine(sum_xy,np.sqrt(sum_xx),np.sqrt(sum_yy))
    return user_pair, (cos_sim,n)

def cosine(dot_product,rating_norm_squared,rating2_norm_squared):
    '''
    The cosine between two vectors X, Y
       dotProduct(X, Y) / (norm(X) * norm(Y))
    '''
    numerator = dot_product
    denominator = rating_norm_squared * rating2_norm_squared

    return (numerator / (float(denominator))) if denominator else 0.0

def keyOnFirstUser(user_pair,movie_sim_data):
    '''
    For each user-user pair, make the first user's id the key
    '''
    (user1_id,user2_id) = user_pair
    return user1_id,(user2_id,movie_sim_data)

def returnUserID(u):
    
    (uid,user_sim) = u
    return uid
	
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, \
            "Usage: CosineSimilarityProg <file>"
        exit(-1)

    sc = SparkContext(appName = "CosineSimilarityProg")
    '''
    Ratings file: Format- user_id,movie_id,rating
    Creates an RDD with each line as element
    '''
    lines = sc.textFile(sys.argv[1])
    '''
    Movie file: Format- movie_id,movie_title,movie_genre
    Creates an RDD with each line as element
    '''
    movies_file = sc.textFile(sys.argv[2])
    '''
    User id of current user
    '''
    user_id_test = int(sys.argv[3]);
    movies_list = movies_file.map(parseMovie).cache()
    ''' 
    Parse the vector with movie_id as the key:
        movie_id -> (user_id,rating)
    '''
    movie_user = lines.map(parseOnMovieId).cache()

    '''
        movie_id -> ((user_1,rating),(user2,rating))
    '''
    movie_user_pairs = movie_user.join(movie_user)

    '''
		(user1_id,user2_id) -> [(rating1,rating2),
                                (rating1,rating2),
                                (rating1,rating2),
                                ...] 
    '''
    user_movie_rating_pairs = movie_user_pairs.map(lambda p: keyOnUserPair(p[0],p[1])).filter(lambda p: p[0][0] != p[0][1]).groupByKey()
    #print user_movie_rating_pairs.take(200)
    '''
    Calculate the cosine similarity for each user pair:
        (user1,user2) ->    (similarity,co_raters_count)
    '''
    user_pair_sims = user_movie_rating_pairs.map(lambda p: calcSim(p[0],p[1]))
    print "COSINE VALUES:::::::::::::::::::::::"
    user_pair_sims = user_pair_sims.map(lambda x: x).sortByKey()
    print user_pair_sims.take(100)
    user_with_firstKey = user_pair_sims.map(lambda p:keyOnFirstUser(p[0],p[1]))
    #print user_with_firstKey.take(100)
    user_groupByKey = user_with_firstKey.groupByKey().map(lambda x:x).sortByKey().collect()
    #print user_groupByKey
    '''
    Pre-processing data: user_id,([(user_id2,(cosine_value,#of common movies)),.....])
    '''
    #Current user_id 
    user = user_groupByKey[user_id_test-1]
    b = sc.parallelize(user[1])
    
    
    
    '''
    Consider users with atleast 10 common movies
    '''
    bgreat10 = b.filter(lambda a:a[1][1]>9)
    bSorted = bgreat10.sortBy(lambda a:-(a[1][0]))
    print "Users which have high cosine similarity value and atleast 10 common movies:"
    for u in bSorted.collect():
		print u
    
    user_common_list= bSorted.map(lambda p:returnUserID(p))
    #print "Top 10 user id's with higher cosine value",user_common_list.take(10)
    '''
    Average rating is calculated in the preliminary data inspection
    '''
    avg_rating=2.5
    '''
    Retrieve the movie_id and rating for each user_id. Group the result on key(user_id)
    '''
    user_item_list = lines.map(parseOnUser).filter(lambda p:p[1][1]>=avg_rating).cache()
    #print user_item_list.take(5)
    user_movie_pair = user_item_list.groupByKey().sortByKey()
    #print user_movie_pair.take(10)
    
    '''
    Retrieve the list of movies based on Users with highest cosine value with the user_id_test
    '''
    um_dict = []
    for index in user_common_list.collect():
		temp_list = []
		#print index,"::::"
		temp_list = user_movie_pair.filter(lambda p:p[0]==index).map(lambda p:p[1]).collect()
		temp = sc.parallelize(temp_list).collect()
		#print temp
		for u in temp:
			u = sc.parallelize(u).collect()
			um_dict.append(u)
			#print u.take(10)
		#ui_dict = ui_dict.append(temp_list)
    #print um_dict
    ''' 
    Sort the list in descending value of ratings
    '''

    um_dict_sorted = sorted(um_dict[0], key=lambda s: -s[1])
    #print um_dict_sorted
    final_list = um_dict_sorted[:50]
    print "Final List of Movies in descending order based on rating"
    print final_list
    '''
    Display the Top N Movies Recommendation
    '''
	
	
    print "User ",user_id_test
    temp_list = user_movie_pair.filter(lambda p:p[0]==user_id_test).map(lambda p:p[1]).collect()
    temp = sc.parallelize(temp_list).collect()
    #print temp
    user_list = []
    for u in temp:
		u = sc.parallelize(u).collect()
		user_list.append(u)
    print user_list
    for i in user_list[0]:
    	tempName = movies_list.filter(lambda p:p[0]==i[0]).map(lambda p:p[1]).collect();
    	print tempName
	
    print "Top 50 Recommendation:"
    for i in final_list:
		tempName = movies_list.filter(lambda p:int(p[0])==int(i[0])).map(lambda p:p[1]).collect();
		print tempName
	
    sc.stop()