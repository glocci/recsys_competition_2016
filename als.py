#! /usr/bin/env python

import pandas as pd
import scipy.sparse as sparse
import numpy as np
import random
import implicit
import time
import csv
import sys
import os.path
import plac
from sklearn import metrics
from joblib import Parallel, delayed
from scipy.sparse.linalg import spsolve
from sklearn.preprocessing import MinMaxScaler
import csv
import math
import operator
import pickle
from collections import defaultdict
from joblib import Parallel, delayed
from math import radians, cos, sin, asin, sqrt
import random

with open('DATA/interactions.csv', 'rb') as f:
    reader = csv.reader(f, delimiter='\t')
    interactions = list(reader)[1:]
with open('DATA/item_profile.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    items = list(reader)
    item_headers = items[0]
    items = items[1:]
with open('DATA/user_profile.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    users = list(reader)[1:]
with open('DATA/target_users.csv', 'rb') as f:
    reader = csv.reader(f, delimiter='\t')
    targets = list(reader)[1:]





def wait():

   raw_input("Press Enter to continue...")
def toInt(str):

    try:
        return int(str)
    except ValueError, ex:
      
        return int(-1)           
def toFloat(str):
  
    try:
        return float(str)
    except ValueError, ex:
  
        return float(-1)         


#######################################################
#                  BUILD FUNCTIONS                    #
#######################################################
def b_users(users):
   
   user_feature_dict = {}
   
   for user in users: 
       newUser = []
       newUser.append(toInt(user[0]))
       newUser.append(user[1])
       if toInt(user[2])==0:
         user[2]=3 
       newUser.append(toInt(user[2]))
       newUser.append(toInt(user[3]))
       newUser.append(toInt(user[4]))
       if user[5] == 'de':
           newUser.append(1)
       elif user[5] == 'at':
           newUser.append(2)
       elif user[5] == 'ch':
           newUser.append(3)
       elif user[5] == 'non_dach':
           newUser.append(4)
       else:
           newUser.append(1)
       newUser.append(toInt(user[6]))
       newUser.append(toInt(user[7]))
       newUser.append(toInt(user[8]))
       newUser.append(toInt(user[9]))
       newUser.append(toInt(user[10]))
       newUser.append(user[11]) 
       features = {}
       user_id = newUser[0]
       features['user_id']= user_id
       jobroles = newUser[1]
       features['jobroles'] = jobroles
       career_level = newUser[2]
       features['career_level'] = career_level
       discipline_id = newUser[3]
       features['discipline_id'] = discipline_id
       industry_id = newUser[4] 
       features['industry_id'] = industry_id
       country = newUser[5]
       features['country'] = country
       region = newUser[6]
       features['region'] = region
       experience_n_entries_class = newUser[7]
       features['experience_n_entries_class'] = experience_n_entries_class
       experience_years_experience = newUser[8]
       features['experience_years_experience'] = experience_years_experience
       experience_years_in_current = newUser[9]
       features['experience_years_in_current'] = experience_years_in_current
       edu_degree = newUser[10]
       features['edu_degree'] = edu_degree
       edu_fieldofstudies = newUser[11]
       features['edu_fieldofstudies'] = edu_fieldofstudies
       features['list'] = newUser
       user_feature_dict[user_id] = features
       user_feature_dict[user_id]['features_norm'] = 0
   
   return user_feature_dict                    
def b_target_users(targets):
   
    target_set = []
   
    for x in targets:
        target_set.append(toInt(x[0]))
   
    return target_set
def users_interactions(interactions):
   
   users_interactions = set()

   
   for inter in interactions:
      users_interactions.add(toInt(inter[0]))
      item_frequency_dict[toInt(inter[1])] += 1
   
   return users_interactions             
def b_items(items):
   
   item_feature_dict = {}
   
   for item in items:
       newItem = []
       newItem.append(toInt(item[0]))
       newItem.append(item[1])
       newItem.append(toInt(item[2]))
       newItem.append(toInt(item[3]))
       newItem.append(toInt(item[4]))
       if item[5] == 'de':
           newItem.append(1)
       elif item[5] == 'at':
           newItem.append(2)
       elif item[5] == 'ch':
           newItem.append(3)
       elif item[5] == 'non_dach':
           newItem.append(4)
       else:
           newItem.append(0)
       newItem.append(toInt(item[6]))
       newItem.append(toFloat(item[7]))
       newItem.append(toFloat(item[8]))
       newItem.append(toInt(item[9]))
       newItem.append(item[10])
       newItem.append(toInt(item[11]))
       newItem.append(toInt(item[12]))
       features = {}
       item_id = newItem[0]
       features['item_id'] = item_id
       title = newItem[1]
       features['title'] = title
       title_split = title.split(',')
       career_level = newItem[2]
       features['career_level'] = career_level
       discipline_id = newItem[3]
       features['discipline_id'] = discipline_id
       industry_id = newItem[4] 
       features['industry_id'] = industry_id
       country = newItem[5]
       features['country'] = country
       region = newItem[6]
       features['region'] = region
       latitude = newItem[7]
       features['latitude'] = latitude
       longitude = newItem[8]
       features['longitude'] = longitude
       employment = newItem[9]
       features['employment'] = employment
       tags = newItem[10]
       features['tags'] = tags
       tags_split = tags.split(',')
       tags_split = tags.split(',')
       created_at = newItem[11]
       features['created_at'] = created_at
       active_during_test = newItem[12]
       features['active_during_test'] = active_during_test
       features['list'] = newItem
       item_feature_dict[item_id] = features
       item_feature_dict[item_id]['features_norm'] = 0
   
   return item_feature_dict
def b_recommendable_items():
   rec = []
   
   for item in item_feature_dict.keys():
      if item_feature_dict[item]['active_during_test'] == 1 and item_time_dict[item] >= max_inter - 60*60*24*30:
         rec.append(item)

   return set(rec)

def b_item_clicks(interactions , days):

   item_clicks = {item : 0.0 for item in item_feature_dict.keys()}
   
   for inter in interactions:
      time = toInt(inter[3])*1.0
      item = toInt(inter[1])
   
      if time >= max_inter - 60*60*24*days:
         item_clicks[item] += 1

   return item_clicks


def b_latest_interaction(interactions , max_inter):
   
   item_time_dict = {item : 0 for item in item_feature_dict.keys()}
   
   for inter in interactions:
      time = toInt(inter[3])
      item = toInt(inter[1])
   
      if time > max_inter:
         max_inter = time
      if item in item_time_dict:
   
         if time > item_time_dict[item]:
            item_time_dict[item] = time
      else:
         item_time_dict[item] = time
   
   return item_time_dict , max_inter
def user_items_dict(interactions):
   
   user_items_dict = defaultdict(set)
   
   for u in user_feature_dict:
      user_feature_dict[u]['explicit_rating'] = {}
      user_feature_dict[u]['implicit_rating'] = {}
      user_feature_dict[u]['total_number_of_interactions'] = 0.0
   
   for i in item_feature_dict: 
      item_feature_dict[i]['total_number_of_interactions'] = 0.0
   
   for inter in interactions:
      user_items_dict[toInt(inter[0])].add(toInt(inter[1]))
      user_feature_dict[toInt(inter[0])]['total_number_of_interactions'] += 1.0
      item_feature_dict[toInt(inter[1])]['total_number_of_interactions'] += 1.0
      if toInt(inter[1]) not in user_feature_dict[toInt(inter[0])]['explicit_rating']:
         user_feature_dict[toInt(inter[0])]['implicit_rating'][toInt(inter[1])] = 0.0
         user_feature_dict[toInt(inter[0])]['explicit_rating'][toInt(inter[1])] = toInt(inter[2])
      elif user_feature_dict[toInt(inter[0])]['explicit_rating'][toInt(inter[1])] < toInt(inter[2]):
         user_feature_dict[toInt(inter[0])]['explicit_rating'][toInt(inter[1])] = toInt(inter[2])
   
   for u in user_feature_dict:
   
    for i in user_feature_dict[u]['implicit_rating']:
      user_feature_dict[u]['implicit_rating'][i] = math.log(len(interactions)/ item_feature_dict[i]['total_number_of_interactions'],10)  
   
   for u in user_feature_dict:
    sum_implicit = 0.0
   
    for i in user_feature_dict[u]['implicit_rating']:
      sum_implicit +=   user_feature_dict[u]['implicit_rating'][i]*user_feature_dict[u]['implicit_rating'][i]
    user_feature_dict[u]['norm_ratings'] = sum_implicit
   
   return user_items_dict   
def target_scores():
   target_dict = {}
   for target in target_users:
      ranked = {}
      target_dict[target] = ranked
   return target_dict


#######################################################
#                       MAIN                          #
#######################################################

title_items_dict = defaultdict(set)
item_feature_dict = b_items(items)
user_feature_dict = b_users(users)

item_frequency_dict = {item : 0 for item in item_feature_dict.keys()} 

target_users = b_target_users(targets)
users_interactions = users_interactions(interactions)
user_items_dict = user_items_dict(interactions)    


print "build_items_dicts_and_sets..."
item_time_dict , max_inter = b_latest_interaction(interactions , 0)
rec= b_recommendable_items()
item_clicks = b_item_clicks(interactions , 4)
item_clicks_for_resort = b_item_clicks(interactions , 4)

sorted_top_pop = sorted(item_frequency_dict ,key=item_frequency_dict.get, reverse=True)

top_pop_to_exclude = sorted_top_pop[:13]




def rec_items(j, customer_id, mf_train, user_vecs, item_vecs, customer_list, item_list, num_items = 300, filterfunc=None):

    print "user number : " , j
    cust_ind = np.where(customer_list == customer_id)[0][0] # Returns the index row of our customer id
    pref_vec = mf_train[cust_ind,:].toarray() # Get the ratings from the training set ratings matrix
    pref_vec = pref_vec.reshape(-1) + 1 # Add 1 to everything, so that items not purchased yet become equal to 1
    pref_vec[pref_vec > 1] = 0 # Make everything already purchased zero

    rec_vector = user_vecs[cust_ind,:].dot(item_vecs.T) # Get dot product of user vector and all item vectors
    # Scale this recommendation vector between 0 and 1
    min_max = MinMaxScaler()
    rec_vector_scaled = min_max.fit_transform(rec_vector.reshape(-1,1))[:,0]
    recommend_vector = pref_vec*rec_vector_scaled
    # Items already purchased have their recommendation multiplied by zero
    rec_list = []
    product_idx = np.argsort(recommend_vector)[::-1]
    sorted_scores = np.sort(recommend_vector)[::-1][:300]
    # of best recommendations
    i = 0
    for index in product_idx:
        code = item_list[index]
        if toInt(code) in rec:
            rec_list.append(code)
            i += 1
        if i == num_items:
            break
    items_with_rating = []
    for i in range(len(rec_list)):
        item_with_rating = str(rec_list[i]) + ';' + str(sorted_scores[i])
        items_with_rating.append(item_with_rating)

    suggestions = str(' '.join(map(str, items_with_rating)))
    submission = str(customer_id) + ',' + suggestions + '\n'

    return submission

@plac.annotations(
    als_factors=('Number of latent factors that ALS should compute', 'option', 'f', int),
    als_alpha=('Used to compute confidence intervals in user-item interaction sparse matrix', 'option', 'a', float),
    als_lambda=('Regularization term used in objective function that ALS minimizes', 'option', 'l', float),
    als_iterations=('Number of iterations to perform in the ALS algorithm', 'option', 'i', int),
    outfile=('Output file where to write submission data', 'option', 'o', str)
)

def main( outfile = '', als_factors = 400, als_lambda = 0.15, als_alpha = 40, als_iterations = 50):
    start_time = time.time()
    interactions = "interactions.csv"
    testset = "target_users.csv"
    interactions_data = pd.read_csv(interactions, sep='\t')
    jobs_data = pd.read_csv("item_profile.csv", sep=',')
    users_data = pd.read_csv("user_profile.csv", sep=',')

    interactions_data = interactions_data[['item_id', 'interaction_type', 'user_id']]

    group = interactions_data.groupby(['item_id', 'user_id']).count().reset_index()
    jobs_not_avail = jobs_data.loc[jobs_data['active_during_test'] == 0]
    items = list(jobs_data.item_id.unique())
    users = list(np.sort(users_data.user_id.unique()))
    types = list(group.interaction_type)
    rows = group.user_id.astype('category', categories=users).cat.codes
    cols = group.item_id.astype('category', categories=items).cat.codes
    ints_sparse = sparse.csr_matrix((types, (rows, cols)), shape=(len(users), len(items)))

    matrix_size = ints_sparse.shape[0] * ints_sparse.shape[1]
    num_interactions = len(ints_sparse.nonzero()[0])
    sparsity = 100 * (1 - (num_interactions / float(matrix_size)))

    print "User-item interaction matrix sparsity: %f%%" % sparsity

    print "Starting training."

    start_train_time = time.time()

    user_vecs, item_vecs = implicit.alternating_least_squares((ints_sparse * als_alpha).astype('double'), factors = als_factors, regularization = als_lambda, iterations = als_iterations)

    end_train_time = time.time()

    print "Training completed, it took %s seconds." % (end_train_time - start_train_time)

    print "Predicting Recommendations."

    users_arr = np.array(users)
    items_arr = np.array(items)

    submission_dict = list()
    test_set = pd.read_csv(testset, sep='\t')

    start_rec_time = time.time()
to_submit  =  (Parallel(n_jobs=-1)(delayed(rec_items)(j , int(target), ints_sparse, user_vecs, item_vecs, users_arr, items_arr, num_items = 300)for j,target in enumerate(test_set.user_id)))


   
    end_rec_time = time.time()

    print "Prediction completed, it took %s seconds." % (end_rec_time - start_rec_time)

    print "Writing submission csv file."


    filename = 'ALS.csv'
    f2 = open(filename, 'w')
    f2.write('user_id,recommended_items\n')
    for sub in to_submit:
         f2.write(sub)



if __name__ == '__main__':
    plac.call(main)
