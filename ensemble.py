import csv
import math
import operator
import pickle
from collections import defaultdict
from joblib import Parallel, delayed
from math import radians, cos, sin, asin, sqrt
import random

with open('interactions.csv', 'rb') as f:
    reader = csv.reader(f, delimiter='\t')
    interactions = list(reader)[1:]
with open('item_profile.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    items = list(reader)
    item_headers = items[0]
    items = items[1:]
with open('user_profile.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    users = list(reader)[1:]
with open('target_users.csv', 'rb') as f:
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

sorted_top_pop = sorted(item_frequency_dict ,key=item_frequency_dict.get, reverse=True)

top_pop_to_exclude = sorted_top_pop[:13]

#######################################################
#                   RECOMMEND                         #
#######################################################
with open('SUBMISSIONS/SIM_CB_II_NO_FALLBACK.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CB_II_NO_FALLBACK = list(reader)[1:]
with open('SUBMISSIONS/SIM_CB_II_NO_FALLBACK_JACCARD.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CB_II_NO_FALLBACK_JACCARD = list(reader)[1:]
with open('SUBMISSIONS/SIM_CB_UU.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CB_UU_NO_FALLBACK = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_II_IMPLICIT_IDF_ITEMS_NO_FALLBACK.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_II_IMPLICIT_NO_FALLBACK = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_II_IMPLICIT_IDF_ITEMS_NO_FALLBACK_JACCARD.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_II_IMPLICIT_NO_FALLBACK_JACCARD = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_II_IMPLICIT_IDF_USERS_NO_FALLBACK.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_II_IMPLICIT_IDF_USERS_NO_FALLBACK = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_II_IMPLICIT_IDF_USERS_NO_FALLBACK_JACCARD.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_II_IMPLICIT_IDF_USERS_NO_FALLBACK_JACCARD = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_II_NO_IMPLICIT_NO_FALLBACK.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_II_NO_IMPLICIT_NO_FALLBACK = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_II_NO_IMPLICIT_NO_FALLBACK_JACCARD.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_II_NO_IMPLICIT_NO_FALLBACK_JACCARD = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_UU_IMPLICIT_NO_FALLBACK.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_UU_IMPLICIT_NO_FALLBACK = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_UU_IMPLICIT_NO_FALLBACK_JACCARD.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_UU_IMPLICIT_NO_FALLBACK_JACCARD = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_UU_NO_IMPLICIT_NO_FALLBACK.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_UU_NO_IMPLICIT_NO_FALLBACK = list(reader)[1:]
with open('SUBMISSIONS/SIM_CF_UU_NO_IMPLICIT_NO_FALLBACK_JACCARD.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    CF_UU_NO_IMPLICIT_NO_FALLBACK_JACCARD = list(reader)[1:]
with open('SUBMISSIONS/SIM_FIRST_ENSEMBLE.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    SIM_FIRST_ENSEMBLE = list(reader)[1:]
with open('SUBMISSIONS/ALS.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    ALS = list(reader)[1:]
with open('SUBMISSIONS/WARP.csv', 'rb') as f:
    reader = csv.reader(f, delimiter=',')
    WARP = list(reader)[1:]



def ensemble_mean(CF_II,CF_UU,CB_II,CB_UU ):

   submissions_first_layer = []
   target_scores_first_layer = target_scores()

   for sub in ALS:
      user = toInt(sub[0])
      items = sub[1].split(' ')

      if (len(items)<=1) or user in unknown_users:
         continue
      max_score = toFloat(items[0].split(';')[1])
      if max_score != 0:
         for i in range(len(items)):
            item = toInt(items[i].split(';')[0])
            score = toFloat(items[i].split(';')[1])/max_score
            if item != -1 and item != 0 and item in rec and item not in top_pop_to_exclude:
               if item in target_scores_first_layer[user]:
                  target_scores_first_layer[user][item] += score*5
               else:
                  target_scores_first_layer[user][item] = score*5

   for sub in CF_II:
      user = toInt(sub[0])
      items = sub[1].split(' ')

      if (len(items)<=1) or user in unknown_users:
         continue
      max_score = toFloat(items[0].split(';')[1])
      if max_score != 0:
         for i in range(len(items)):
            item = toInt(items[i].split(';')[0])
            score = toFloat(items[i].split(';')[1])/max_score
            if item != -1 and item != 0 and item in rec and item not in top_pop_to_exclude:
               if item in target_scores_first_layer[user]:
                  target_scores_first_layer[user][item] += score*9
               else:
                  target_scores_first_layer[user][item] = score*9

   for sub in WARP:
      user = toInt(sub[0])
      items = sub[1].split(' ')

      if (len(items)<=1) or user in unknown_users:
         continue
      if (len(items[0])==2):
         max_score = toFloat(items[0].split('@')[1])
         if max_score != 0:
            for i in range(len(items)):
               item = toInt(items[i].split('@')[0])
               score = toFloat(items[i].split('@')[1])/max_score
               if item != -1 and item != 0 and item in rec and item not in top_pop_to_exclude:
                  if item in target_scores_first_layer[user]:
                     target_scores_first_layer[user][item] += score*3
                  else:
                     target_scores_first_layer[user][item] = score*3


   for sub in CF_UU:
      user = toInt(sub[0])
      items = sub[1].split(' ')
      if (len(items)<=1):
         continue
      max_score = toFloat(items[0].split(';')[1])
      for i in range(len(items)):
         item = toInt(items[i].split(';')[0])
         score = toFloat(items[i].split(';')[1])/max_score
         if item != -1 and item != 0 and item in rec and item not in top_pop_to_exclude:
            if item in target_scores_first_layer[user]:
               target_scores_first_layer[user][item] += score*1
            else:
               target_scores_first_layer[user][item] = score*1

   for sub in CB_II:
      user = toInt(sub[0])
      items = sub[1].split(' ')
      if (len(items)<=1):
         continue
      max_score = toFloat(items[0].split(';')[1])
      for i in range(len(items)):
         item = toInt(items[i].split(';')[0])
         score = toFloat(items[i].split(';')[1])/max_score
         if item != -1 and item != 0 and item in rec and item not in top_pop_to_exclude:
            if item in target_scores_first_layer[user]:
               target_scores_first_layer[user][item] += score*10
            else :
               target_scores_first_layer[user][item] = score*10

   for sub in CB_UU:
      user = toInt(sub[0])
      items = sub[1].split(' ')
      if (len(items)<=1):
         continue
      max_score = toFloat(items[0].split(';')[1])
      for i in range(len(items)):
         item = toInt(items[i].split(';')[0])
         score = toFloat(items[i].split(';')[1])/max_score
         if item != -1 and item != 0 and item in rec and item not in top_pop_to_exclude:
            if item in target_scores_first_layer[user]:
               target_scores_first_layer[user][item] += score*0.1
            else :
               target_scores_first_layer[user][item] = score*0.1

   for user in target_scores_first_layer:

      sorted_by_rating = sorted(target_scores_first_layer[user], key=target_scores_first_layer[user].get, reverse=True)

      items_with_rating = []
      for item in sorted_by_rating:
         item_with_rating = str(item) + ';' + str( target_scores_first_layer[user][item])
         items_with_rating.append(item_with_rating)

      suggestions = str(' '.join(map(str, items_with_rating)))
      submission = str(user) + ',' + suggestions
      submissions_first_layer.append(submission)



   return submissions_first_layer





def final_ensemble(final_ensemble_list ):


   submissions_first_layer = []
   target_scores_first_layer = target_scores()


   for submissions in final_ensemble_list:
      weight = submissions[1]
      to_rank = submissions[0]
      a = 0
      for sub in to_rank:
         sub = sub.split(',')
         user = toInt(sub[0])
         items = sub[1].split(' ')
         if (len(items)<=1):
            continue
         max_score = toFloat(items[0].split(';')[1])
         for i in range(len(items)):
            item = toInt(items[i].split(';')[0])
            score = toFloat(items[i].split(';')[1])/max_score
            if item != -1 and item != 0 and item in rec and item not in top_pop_to_exclude:
               if item in target_scores_first_layer[user]:
                  target_scores_first_layer[user][item] += score*weight  
               else:
                  target_scores_first_layer[user][item] = score*weight  

   


   for user in target_scores_first_layer:

      sorted_by_rating = sorted(target_scores_first_layer[user], key=target_scores_first_layer[user].get, reverse=True)

      items_with_rating = []
      for item in sorted_by_rating:
         item_with_rating = str(item) + ';' + str( target_scores_first_layer[user][item])
         items_with_rating.append(item_with_rating)

      suggestions = str(' '.join(map(str, items_with_rating)))
      submission = str(user) + ',' + suggestions
      submissions_first_layer.append(submission)



   return submissions_first_layer 
 


def resort(submission):

   submissions_first_layer = []
   target_scores_first_layer = target_scores()
   for sub in submission:
         sub = sub.split(',')
         user = toInt(sub[0])
         items = sub[1].split(' ')
         if (len(items)<=1):
            continue
         max_score = toFloat(items[0].split(';')[1])

         for i in range(len(items)):
            item = toInt(items[i].split(';')[0])
            score = toFloat(items[i].split(';')[1])/max_score
            if item != -1 and item != 0 and item in rec and item not in top_pop_to_exclude:
                  if item in target_scores_first_layer[user]:
                     target_scores_first_layer[user][item] += (1-0.05*i) + score*(float(item_clicks[item])/125)

                  else:
                     target_scores_first_layer[user][item] = (1-0.05*i) + score*(float(item_clicks[item])/125)

   for user in target_scores_first_layer:
      sorted_by_rating = sorted(target_scores_first_layer[user], key=target_scores_first_layer[user].get, reverse=True)
      suggestions = str(' '.join(map(str, sorted_by_rating[:5])))
      submission = str(user) + ',' + suggestions + '\n' 
      submissions_first_layer.append(submission)

   return submissions_first_layer  

  



known_users = set()
unknown_users = set()
print "split_known_and_unknown_users..."
for user in target_users:
    u = user_items_dict[user]
    if len(u) != 0 :
        known_users.add(user)
    else:
        unknown_users.add(user)

FINAL_ENSEMBLE_LIST = []

submission_ensemble_mean_implicit = ensemble_mean(CF_II_IMPLICIT_NO_FALLBACK,CF_UU_IMPLICIT_NO_FALLBACK,CB_II_NO_FALLBACK , CB_UU_NO_FALLBACK )
FINAL_ENSEMBLE_LIST.append([submission_ensemble_mean_implicit,1])
submission_ensemble_mean_no_implicit = ensemble_mean(CF_II_NO_IMPLICIT_NO_FALLBACK,CF_UU_NO_IMPLICIT_NO_FALLBACK,CB_II_NO_FALLBACK , CB_UU_NO_FALLBACK)
FINAL_ENSEMBLE_LIST.append([submission_ensemble_mean_no_implicit,1])
submission_esemble_mean_implicit_jaccard = ensemble_mean(CF_II_IMPLICIT_NO_FALLBACK_JACCARD,CF_UU_IMPLICIT_NO_FALLBACK_JACCARD,CB_II_NO_FALLBACK_JACCARD , CB_UU_NO_FALLBACK)
FINAL_ENSEMBLE_LIST.append([submission_esemble_mean_implicit_jaccard,1])
submission_ensemble_mean_no_implicit_jaccard = ensemble_mean(CF_II_NO_IMPLICIT_NO_FALLBACK_JACCARD,CF_UU_NO_IMPLICIT_NO_FALLBACK_JACCARD,CB_II_NO_FALLBACK_JACCARD , CB_UU_NO_FALLBACK)
FINAL_ENSEMBLE_LIST.append([submission_ensemble_mean_no_implicit_jaccard,1])
submission_ensemble_mean_implicit_idf_users = ensemble_mean(CF_II_IMPLICIT_IDF_USERS_NO_FALLBACK,CF_UU_IMPLICIT_NO_FALLBACK,CB_II_NO_FALLBACK , CB_UU_NO_FALLBACK )
FINAL_ENSEMBLE_LIST.append([submission_ensemble_mean_implicit_idf_users,1])
submission_ensemble_mean_implicit_idf_users_jaccard = ensemble_mean(CF_II_IMPLICIT_IDF_USERS_NO_FALLBACK_JACCARD,CF_UU_IMPLICIT_NO_FALLBACK,CB_II_NO_FALLBACK , CB_UU_NO_FALLBACK )
FINAL_ENSEMBLE_LIST.append([submission_ensemble_mean_implicit_idf_users_jaccard,1])



final = final_ensemble( FINAL_ENSEMBLE_LIST )
resorted = resort(final)

print "open_file"
filename = 'NOMESUB.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in resorted:
      f2.write(sub)
