#modules
import re
import random
import numpy as np
import matplotlib.cm as cm
import statsmodels.api as sm
from collections import Counter
from collections import defaultdict
from scipy.stats.stats import pearsonr
import matplotlib.pyplot as plt
from math import sqrt
import time
import json
from scipy import spatial

# functions
def sim_pearson(prefs,p1,p2):
    a = [(i, prefs[p1][i], prefs[p2][i]) for i in prefs[p1] if i in prefs[p2]]
    if a and len(a) > 1:
        b = np.array(a)[:,1:].astype(float)
        return pearsonr(b[:,0], b[:,1])[0]
    else:
        return 0

def topMatches(prefs,p1,n=5, similarity=sim_pearson):
    scores=[(similarity(prefs,p1,other),other)
                   for other in prefs if other!=p1]
    scores.sort()
    scores.reverse( )
    return scores[0:n]

def calculateSimilarItems(prefs,n=10):
    result={}
    c=0
    for item in prefs:
        c+=1
        if c%1000==0: print "%d / %d" % (c,len(prefs))
        scores=topMatches(prefs,item,n=n,similarity=sim_pearson)
        result[item]=scores
    return result

# main dataset
user_dict={}
business_dict={}
with open('yelp_academic_dataset_review.json') as f:
    for line in f:
        line = json.loads(line)
        user = str(line['user_id'])
        business = str(line['business_id'])
        rate = line['stars']
        if business not in business_dict:
            business_dict[business]={}
        else:
            business_dict[business][user]=rate
        if user not in user_dict:
            user_dict[user]={}
        else:
            user_dict[user][business]=rate

#prepare item similarity matrix, this may takes a while
itemsim=calculateSimilarItems(business_dict)

#hot businesses to be recommended when there is no record
topshops={}
for i in business_dict:
    rates=business_dict[i].values()
    mean=np.mean(rates)
    if mean ==5:
        topshops[i]=len(rates)
topshops = [(score,item) for item,score in topshops.items()]
topshops.sort()
topshops.reverse()
topshops = [(5.0,j) for i,j in topshops[:5]]

#match user and business information
B = {}
with open('yelp_academic_dataset_business.json') as f:
    for line in f:
        line = json.loads(line)
        bid = line["business_id"]
        ad  = line["full_address"]
        B[bid] = ad
U = {}
with open('yelp_academic_dataset_user.json') as f:
    for line in f:
        line = json.loads(line)
        uid = line["user_id"]
        name  = line["name"]
        U[uid] = name

def getRecommendedItems(prefs,itemMatch,user):
    userRatings=prefs[user]
    scores={}
    totalSim={}
    # Loop over items rated by this user
    for (item,rating) in userRatings.items():
             # Loop over items similar to this one
             for (similarity,item2) in itemMatch[item]:
                    # Ignore if this user has already rated this item
                    if item2 in userRatings: continue
                    # Weighted sum of rating times similarity
                    scores.setdefault(item2,0)
                    scores[item2]+=similarity*rating
                    # Sum of all the similarities
                    totalSim.setdefault(item2,0)
                    totalSim[item2]+=similarity
    # Divide each total score by total weighting to get an average 
    rankings=[(score/totalSim[item],item) for item,score in scores.items() if score>0]
    # Return the rankings from highest to lowest 
    rankings.sort( )
    rankings.reverse( )
    if rankings:
        return  [(i,B[j]) for i,j in rankings[:5]]
    else:
        return  [(i,B[j]) for i,j in topshops]

# demo
getRecommendedItems(user_dict,itemsim,random.sample(user_dict.keys(),1)[0]) 