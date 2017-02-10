import csv
import math
import operator
import pickle
from collections import defaultdict
from joblib import Parallel, delayed
from math import radians, cos, sin, asin, sqrt

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





#######################################################
#                  GLOBAL VARIABLES                   #
#######################################################

user_norm = {}         # user norm for content based (see b_u_i_profile function)
user_profile = {}      # user profile  for content based (see b_u_i_profile function)
item_norm = {}         # item norm for content based (see b_u_i_profile function)
item_profile = {}      # item profile  for content based (see b_u_i_profile function)
known_users = set()    # target users with interactions
unknown_users = set()  # target users without interactions
rec_known = []         # recommendation for target users with interactions
rec_unknown = []       # recommendation for target users without interactions
users_with_zero_score = set() # users with 0 score in rec algorithm
problematic_users = set()     # users with 0 score rec for zero ratings
users_scored = set()          # user scored

title_users_dict = defaultdict(set)        #users in which titles appear dictionary 
user_titles_dict = defaultdict(set) 
user_title_frequency_dict = {}             #number of time user-title appear 
user_jobroles_dict = defaultdict(set)      #titles in which users appear dictionary    
user_edu_field_of_studies_dict = defaultdict(set)
jobrole_users_dict = defaultdict(set)                                                                                        
user_jobroles_frequency_dict = {}                                         
jobrole_users_dict = defaultdict(set)                                                                                        
user_jobroles_frequency_dict = {}  

career_level_users_dict = defaultdict(set)    
discipline_id_users_dict = defaultdict(set)
industry_id_users_dict = defaultdict(set)
country_users_dict = defaultdict(set)
region_users_dict = defaultdict(set)
edu_degree_users_dict = defaultdict(set)
edu_fieldofstudies_users_dict = defaultdict(set)

career_level_items_dict = defaultdict(set)    
discipline_id_items_dict = defaultdict(set)
industry_id_items_dict = defaultdict(set)
country_items_dict = defaultdict(set)
region_items_dict = defaultdict(set)

item_jobroles_dict = defaultdict(set)
item_career_level_dict = defaultdict(set)
item_region_dict = defaultdict(set)
item_country_dict = defaultdict(set)
item_discipline_id_dict = defaultdict(set)
item_experience_n_entries_class_dict = defaultdict(set)
item_experience_years_experience_dict = defaultdict(set)
item_experience_years_in_current_dict = defaultdict(set)
item_edu_degree_dict = defaultdict(set)
item_edu_fieldofstudies_dict = defaultdict(set)
item_industry_id_dict = defaultdict(set)

item_items_time_dict = defaultdict(set)

#######################################################
#                  UTILIS FUNCTIONS                   #
#######################################################
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
      
       jobroles_split = jobroles.split(',')
       for j in jobroles_split:
         if j != 0:
            j = toInt(j)
            jobrole_users_dict[j].add(user_id)

       career_level = newUser[2]
       features['career_level'] = career_level

       career_level_users_dict[career_level].add(user_id)

       discipline_id = newUser[3]
       features['discipline_id'] = discipline_id

       discipline_id_users_dict[discipline_id].add(user_id)

       industry_id = newUser[4] 
       features['industry_id'] = industry_id

       industry_id_users_dict[industry_id].add(user_id)

       country = newUser[5]
       features['country'] = country

       country_users_dict[country].add(user_id)

       region = newUser[6]
       features['region'] = region

       region_users_dict[region].add(user_id)

       experience_n_entries_class = newUser[7]
       features['experience_n_entries_class'] = experience_n_entries_class
       experience_years_experience = newUser[8]
       features['experience_years_experience'] = experience_years_experience
       experience_years_in_current = newUser[9]
       features['experience_years_in_current'] = experience_years_in_current
      
       edu_degree = newUser[10]
       features['edu_degree'] = edu_degree

       edu_degree_users_dict[edu_degree].add(user_id)

       edu_fieldofstudies = newUser[11]
       features['edu_fieldofstudies'] = edu_fieldofstudies

       edu_split = edu_fieldofstudies.split(',')
       for e in edu_split:
         if e != 0:
            e1 = toInt(e)
            edu_fieldofstudies_users_dict[e1].add(user_id)


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
   
   return users_interactions
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
   
       for t in title_split:
         if t != 0:
            t = toInt(t)
            title_items_dict[t].add(item_id)
   
       career_level = newItem[2]
       features['career_level'] = career_level

       career_level_items_dict[career_level].add(item_id)

       discipline_id = newItem[3]
       features['discipline_id'] = discipline_id

       discipline_id_items_dict[discipline_id].add(item_id)

       industry_id = newItem[4] 
       features['industry_id'] = industry_id

       industry_id_items_dict[industry_id].add(item_id)

       country = newItem[5]
       features['country'] = country

       country_items_dict[country].add(item_id)
       
       region = newItem[6]
       features['region'] = region

       region_items_dict[region].add(item_id)

       latitude = newItem[7]
       features['latitude'] = latitude
       longitude = newItem[8]
       features['longitude'] = longitude
       employment = newItem[9]
       features['employment'] = employment
       tags = newItem[10]
       features['tags'] = tags
       tags_split = tags.split(',')
     


       for t in tags_split:
         if t != 0:
            t = toInt(t)
            title_items_dict[t].add(item_id)
   
       tags_split = tags.split(',')
       created_at = newItem[11]
       features['created_at'] = created_at
       active_during_test = newItem[12]
       features['active_during_test'] = active_during_test
       features['list'] = newItem
       item_feature_dict[item_id] = features
       item_feature_dict[item_id]['features_norm'] = 0
   
   return item_feature_dict
def b_item_users_list(interations):
    
    item_users_list_dict = defaultdict(set)
    
    for interaction in interactions:
        item_users_list_dict[toInt(interaction[1])].add(toInt(interaction[0]))
    
    return item_users_list_dict
def b_item_features_dict(interactions):

   for interaction in interactions:
        user = toInt(interaction[0])
       
        for j in user_feature_dict[user]['jobroles'].split(','):
            item_jobroles_dict[toInt(interaction[1])].add(toInt(j)) 
        
        item_career_level_dict[toInt(interaction[1])].add(user_feature_dict[user]['career_level'])
        item_region_dict[toInt(interaction[1])].add(user_feature_dict[user]['region'])
        item_country_dict[toInt(interaction[1])].add(user_feature_dict[user]['country'])
        item_discipline_id_dict[toInt(interaction[1])].add(user_feature_dict[user]['discipline_id'])
        item_experience_n_entries_class_dict[toInt(interaction[1])].add(user_feature_dict[user]['experience_n_entries_class'])
        item_experience_years_experience_dict[toInt(interaction[1])].add(user_feature_dict[user]['experience_years_experience'])
        item_experience_years_in_current_dict[toInt(interaction[1])].add(user_feature_dict[user]['experience_years_in_current'])
        item_edu_degree_dict[toInt(interaction[1])].add(user_feature_dict[user]['edu_degree'])
        
        for e in user_feature_dict[user]['edu_fieldofstudies'].split(','):
            item_edu_fieldofstudies_dict[toInt(interaction[1])].add(toInt(e))

        item_industry_id_dict[toInt(interaction[1])].add(user_feature_dict[user]['industry_id'])
def b_item_user_dict(item_users_list):    
    
    itemUsers = {}
    
    for item in item_users_list:
        features = {}
        implicit_rating = {}
        norm = {}
    
        for user in item_users_list[item]:
    
            if item!=0 and item!=-1 and user!=0 and user!=-1:
               implicit_rating[user]= user_feature_dict[user]['implicit_rating'][item]
               norm[user] = implicit_rating[user]*implicit_rating[user]
        features['implicit_rating'] = implicit_rating
        features['item_norm'] = 0 
    
        for u in norm:
         features['item_norm'] += norm[u]
        itemUsers[item]= features
    
    return itemUsers
def b_recommendable_items():
   recommendable_items = []
   rec = []
   recommendable_items_for_collaborative =[]
   
   for item in item_feature_dict.keys():
      if item_feature_dict[item]['active_during_test'] == 1  :
         recommendable_items_for_collaborative.append(item)
      if item_feature_dict[item]['active_during_test'] == 1  :
         recommendable_items.append(item)
      if item_feature_dict[item]['active_during_test'] == 1 and item_time_dict[item] >= max_inter - 60*60*24*5 :
         rec.append(item)

   print str(len(recommendable_items)) + " GENERAL RECOMMENDABLE ITEMS"
   print str(len(recommendable_items_for_collaborative)) + " RECOMMENDABLE ITEMS FOR COLLABORATIVE"

   return set(recommendable_items) , set(recommendable_items_for_collaborative), set(rec)
 
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
def b_interactions():
  
   for user in users_interactions:
      items_of_u = user_items_dict[user]
      title_frequency = {}
  
      for item in items_of_u:
         item_frequency_dict[toInt(item)] += 1.0
  
         for title in item_feature_dict[item]['title'].split(','):  
            title = toInt(title)
  
            if title != 0:
               title_users_dict[title].add(user)
               user_titles_dict[user].add(title)
  
               if title in title_frequency.keys():
                   title_frequency[title] += 1.0
               else:
                   title_frequency[title] = 1.0
  
      user_title_frequency_dict[user] = title_frequency
def b_u_i_profile():
   
   number_of_users = float(len(users_interactions))
   
   for target in user_feature_dict:
      jobrole_tf_idf={}
      edu_idf = {}
      title_frequency = {}
      jobrole_frequency = {}
      user = user_feature_dict[toInt(target)]
      user_id = user['user_id']
      
      for jobrole in user['jobroles'].split(','):
         title = toInt(jobrole)
         if title != 0:
            title_users_dict[title].add(user_id)
            if user_id in user_titles_dict.keys():
               if title in user_title_frequency_dict[user_id].keys():
                  user_title_frequency_dict[user_id][title] += 1.0
               else:
                  user_title_frequency_dict[user_id][title] = 1.0
               user_titles_dict[user_id].add(title)
            else:
               title_frequency[title] = 1.0
               user_title_frequency_dict[user_id] = title_frequency
               user_titles_dict[user_id].add(title)

            if user_id in user_jobroles_dict.keys():
               if title in user_jobroles_frequency_dict[user_id].keys():
                  user_jobroles_frequency_dict[user_id][title] += 1.0
               else:
                  user_jobroles_frequency_dict[user_id][title] = 1.0
               user_jobroles_dict[user_id].add(title)
            else:
               jobrole_frequency[title] = 1.0
               user_jobroles_frequency_dict[user_id] = jobrole_frequency
               user_jobroles_dict[user_id].add(title)
      
      for edu_fieldofstudies in user['edu_fieldofstudies'].split(','):
            edu = toInt(edu_fieldofstudies)
            if edu != 0:
               user_edu_field_of_studies_dict[user_id].add(edu)

      norm = 0.0
      
      title_of_users = user_titles_dict[user_id]
      title_tf_idf={}
      
      for title in title_of_users:
         tf = user_title_frequency_dict[user_id][title]
         idf = math.log(number_of_users/len(title_users_dict[title]),10)
         tf_idf = tf*idf
         norm += tf_idf*tf_idf
         title_tf_idf[title] = tf_idf
      user_norm[user_id] = math.sqrt(norm)   
      user_profile[user_id] = title_tf_idf
      norm = 0.0
      jobrole_of_users = user_jobroles_dict[user_id] 
      edu_of_users = user_edu_field_of_studies_dict[user_id] 

      career_level=user_feature_dict[user_id]['career_level']
      discipline_id=user_feature_dict[user_id]['discipline_id']
      industry_id=user_feature_dict[user_id]['industry_id']
      country=user_feature_dict[user_id]['country']
      region=user_feature_dict[user_id]['region']
      edu_degree=user_feature_dict[user_id]['edu_degree']
      edu_fieldofstudies=user_feature_dict[user_id]['edu_fieldofstudies']

      career_level_idf = math.log(number_of_users/len(career_level_users_dict[career_level]),10)
      discipline_id_idf = math.log(number_of_users/len(discipline_id_users_dict[discipline_id]),10)
      industry_id_idf = math.log(number_of_users/len(industry_id_users_dict[industry_id]),10)
      country_users_idf = math.log(number_of_users/len(country_users_dict[country]),10)
      region_idf = math.log(number_of_users/len(region_users_dict[region]),10)
      edu_degree_idf = math.log(number_of_users/len(edu_degree_users_dict[edu_degree]),10)
   
      user_feature_dict[user_id]['career_level_idf'] = career_level_idf
      user_feature_dict[user_id]['discipline_id_idf'] = discipline_id_idf
      user_feature_dict[user_id]['industry_id_idf'] = industry_id_idf
      user_feature_dict[user_id]['country_idf'] = country_users_idf
      user_feature_dict[user_id]['region_idf'] = region_idf
      user_feature_dict[user_id]['edu_degree_idf'] = edu_degree_idf

      norm +=career_level_idf*career_level_idf
      norm +=discipline_id_idf*discipline_id_idf
      norm +=industry_id_idf*industry_id_idf
      norm +=country_users_idf*country_users_idf
      norm +=region_idf*region_idf
      norm +=edu_degree_idf*edu_degree_idf

      for jobrole in jobrole_of_users:
         tfjb = user_jobroles_frequency_dict[user_id][jobrole]
         idfjb = math.log(number_of_users/len(jobrole_users_dict[jobrole]),10)
         tfjb_idfjb = tfjb*idfjb
         norm += tfjb_idfjb*tfjb_idfjb
         jobrole_tf_idf[jobrole] = tfjb_idfjb

      for edu in edu_of_users:
         idfe = math.log(number_of_users/len(edu_fieldofstudies_users_dict[edu]),10)
         norm += idfe*idfe
         edu_idf[edu] = idfe

      user_feature_dict[user_id]['features_norm'] += math.sqrt(norm)  
      user_feature_dict[user_id]['jobroles_tf_idf'] = jobrole_tf_idf
      user_feature_dict[user_id]['edu_idf'] = edu_idf


   number_of_items = float(len(item_feature_dict.keys()))
   for item in item_feature_dict:
      item_id = item
      titles = item_feature_dict[item_id]['title'].split(',')
      tags = item_feature_dict[item_id]['tags'].split(',')
      titles.extend(tags)
      norm = 0.0
      title_item_frequency = {}
      title_tf_idf={}
      for title in titles:
         title = toInt(title)
         if title != 0:
            if title in title_item_frequency.keys():
               title_item_frequency[title] += 1.0
            else:
               title_item_frequency[title] = 1.0
      for title in set(titles):
         title = toInt(title)
         if title != 0:
            tf = title_item_frequency[title]
            idf = math.log(number_of_items/(len(title_items_dict[title])+1),10)
            tf_idf = tf * idf
            norm += tf_idf*tf_idf
            title_tf_idf[title] = tf_idf
      item_norm[item_id] = math.sqrt(norm)
      item_profile[item_id] = title_tf_idf

      norm = 0.0

      career_level=item_feature_dict[item_id]['career_level']
      discipline_id=item_feature_dict[item_id]['discipline_id']
      industry_id=item_feature_dict[item_id]['industry_id']
      country=item_feature_dict[item_id]['country']
      region=item_feature_dict[item_id]['region']

      career_level_idf = math.log(number_of_items/len(career_level_items_dict[career_level]),10)
      discipline_id_idf = math.log(number_of_items/len(discipline_id_items_dict[discipline_id]),10)
      industry_id_idf = math.log(number_of_items/len(industry_id_items_dict[industry_id]),10)
      country_users_idf = math.log(number_of_items/len(country_items_dict[country]),10)
      region_idf = math.log(number_of_items/len(region_items_dict[region]),10)

      item_feature_dict[item_id]['career_level_idf'] = career_level_idf
      item_feature_dict[item_id]['discipline_id_idf'] = discipline_id_idf
      item_feature_dict[item_id]['industry_id_idf'] = industry_id_idf
      item_feature_dict[item_id]['country_idf'] = country_users_idf
      item_feature_dict[item_id]['region_idf'] = region_idf

      norm +=career_level_idf*career_level_idf
      norm +=discipline_id_idf*discipline_id_idf
      norm +=industry_id_idf*industry_id_idf
      norm +=country_users_idf*country_users_idf
      norm +=region_idf*region_idf
      norm +=edu_degree_idf*edu_degree_idf

      item_feature_dict[item_id]['features_norm'] += math.sqrt(norm)  


def b_idf_users_interactions():
   user_idf = {}
   item_idf_norm = {}
   user_idf_norm = {}

   for user in user_items_dict:
      number_of_interactions = len(user_items_dict[user])
      idf = float(len(interactions))/(number_of_interactions+1)
      user_idf[user] = idf

   for item in item_users_list:
      norm= 0.0
      for user in item_users_list[item]:
         norm += user_idf[user]*user_idf[user]
      item_idf_norm[item] = norm

   for user in user_items_dict:
      norm = 0.0
      for item in user_items_dict[user]:
         norm += user_idf[user]*user_idf[user]
      user_idf_norm[user] = norm

   return user_idf , item_idf_norm , user_idf_norm

###################COLLABORATIVE#######################
def collaborative_similarity_item_item_implicit(item1, item2 , jaccard):


     num = 0.0 
     den = 0.0
     shrink = 20
     users_of_item1 = set(item_users_dict[item1]['implicit_rating'].keys())
     users_of_item2 = set(item_users_dict[item2]['implicit_rating'].keys())
     l1 = len(users_of_item1)
     l2 = len(users_of_item2)
     users_intersection = set.intersection(users_of_item1, users_of_item2)
     
     if len(users_intersection)>0:
        for user in users_intersection:
            num += user_idf[user]*user_idf[user]
        if jaccard:
            den +=item_idf_norm[item1]+ item_idf_norm[item2] -num +shrink 
        else:
            den +=item_idf_norm[item1]+ shrink
        res = num / den
     else:
        res = 0  
     
     return res
def collaborative_similarity_item_item_no_implicit(item1, item2 , jaccard):

     num = 0.0 
     shrink = 20.0
     den = 0.0
     users_of_item1 = set(item_users_dict[item1]['implicit_rating'].keys())
     users_of_item2 = set(item_users_dict[item2]['implicit_rating'].keys())
     l1 = len(users_of_item1)
     l2 = len(users_of_item2)
     users_intersection = set.intersection(users_of_item1, users_of_item2)
     
     if len(users_intersection)>0:
        num += len(users_intersection)*1.0
        if jaccard:
         den += l1+l2-len(users_intersection) + shrink
        else:   
         den += math.sqrt(l1*l2)+ shrink 
        res = num / den
     else:
        res = 0  
     
     return res
def collaborative_similarity_user_user_implicit(user1,user2 , jaccard):


      items_of_target = set()
      items_of_user = set()
      
      if user2 in user_items_dict:
         items_of_target = user_items_dict[user2]
      if user1 in user_items_dict:
         items_of_user = user_items_dict[user1] 

      intersection = set(items_of_user).intersection(items_of_target)
      num = 0.0
      den = 0.0
     
      if len(intersection)>0:
     
         for item in intersection:
            num += user_idf[user1]*user_idf[user2]
         if jaccard:
            den += user_idf_norm[user1] + user_idf_norm[user2] - num + 20
         else:
            den +=  math.sqrt( user_idf_norm[user1]* user_idf_norm[user2]) + 20
         return num/den
      else:
      
         return 0


def collaborative_similarity_user_user_no_implicit(user1,user2 , jaccard):
      items_of_target = set()
      items_of_user = set()
      
      if user2 in user_items_dict:
         items_of_target = user_items_dict[user2]
         l2 = len(items_of_target)
      if user1 in user_items_dict:
         items_of_user = user_items_dict[user1] 
         l1 = len(items_of_user)

      intersection = items_of_user.intersection(items_of_target)
      num = 0.0
      den = 0.0
     
      if len(intersection)>0:
         
         num += len(intersection)*1.0

         if jaccard:
            den +=  l2 + l1 - len(intersection) + 20.0
         else:
            den +=  math.sqrt(l2*l1) + 20.0
         return num/den
      else:
      
         return 0


#######################CONTENT#########################
def content_similarity_item_item(item1, item2):
            
            num = 0.0
            profile_item_to_recommend = item_profile[item1]
            profile_item_of_user = item_profile[item2]
            norm_item_to_recommend = item_norm[item1]
            norm_item_of_user = item_norm[item2]
            title_intersections = set(profile_item_of_user.keys()).intersection(profile_item_to_recommend.keys())
            
            for title in title_intersections:
                  num += profile_item_to_recommend[title] * profile_item_of_user[title]
           
            den = (norm_item_of_user*norm_item_of_user + norm_item_to_recommend*norm_item_to_recommend - num + 9.0)
            
            return num/den
def content_similarity_user_item(user1 , item1 , jaccard):
            
            profile_u = user_profile[user1]
            norm_a = math.sqrt(user_norm[user1]*user_norm[user1] + user_feature_dict[user1]['features_norm']*user_feature_dict[user1]['features_norm'])
            profile_i = item_profile[item1]
            norm_b = math.sqrt(item_norm[item1]* item_norm[item1] +  item_feature_dict[item1]['features_norm']*item_feature_dict[item1]['features_norm'])
            num = 0.0
            title_intersections = set(profile_i.keys()).intersection(profile_u.keys())
            
            for title in title_intersections:
                  num += profile_i[title] * profile_u[title]
            
            career_level_u1 = user_feature_dict[user1]['career_level']
            career_level_i1 = item_feature_dict[item1]['career_level']
            discipline_id_u1 = user_feature_dict[user1]['discipline_id']
            discipline_id_i1 = item_feature_dict[item1]['discipline_id']
            industry_id_u1 = user_feature_dict[user1]['industry_id']
            industry_id_i1 = item_feature_dict[item1]['industry_id']
            country_u1 = user_feature_dict[user1]['country']
            country_i1 = item_feature_dict[item1]['country']
            region_u1 = user_feature_dict[user1]['region']
            region_i1 = item_feature_dict[item1]['region']

            if career_level_u1  ==  career_level_i1:
               num += user_feature_dict[user1]['career_level_idf']*item_feature_dict[item1]['career_level_idf']
            if discipline_id_u1!=0 and discipline_id_i1!=0 and discipline_id_u1==discipline_id_i1:
               num += user_feature_dict[user1]['discipline_id_idf']*item_feature_dict[item1]['discipline_id_idf']
            if industry_id_u1!=0 and industry_id_i1!=0 and industry_id_u1==industry_id_i1:
               num += user_feature_dict[user1]['industry_id_idf']*item_feature_dict[item1]['industry_id_idf']
            if country_u1!=0 and country_i1!=0 and country_u1==country_i1:
               num += user_feature_dict[user1]['country_idf']*item_feature_dict[item1]['country_idf']
            if region_u1!=0 and region_i1!=0 and region_u1==region_i1:
               num += user_feature_dict[user1]['region_idf']*item_feature_dict[item1]['region_idf']
            if jaccard:
               return num/ (norm_a + norm_b)
            else:
               return num/ (norm_a * norm_b + 20.0)
def content_similarity_user_user(user1 , user2 , jaccard):
           
            num = 0.0
            jobroles_u1 = user_feature_dict[user1]['jobroles_tf_idf']
            jobroles_u2 = user_feature_dict[user2]['jobroles_tf_idf']
            career_level_u1 = user_feature_dict[user1]['career_level']
            career_level_u2 = user_feature_dict[user2]['career_level']
            discipline_id_u1 = user_feature_dict[user1]['discipline_id']
            discipline_id_u2 = user_feature_dict[user2]['discipline_id']
            industry_id_u1 = user_feature_dict[user1]['industry_id']
            industry_id_u2 = user_feature_dict[user2]['industry_id']
            country_u1 = user_feature_dict[user1]['country']
            country_u2 = user_feature_dict[user2]['country']
            region_u1 = user_feature_dict[user1]['region']
            region_u2 = user_feature_dict[user2]['region']
            edu_degree_u1 = user_feature_dict[user1]['edu_degree']
            edu_degree_u2 = user_feature_dict[user2]['edu_degree']
            edu_fieldofstudies_u1 = user_feature_dict[user1]['edu_idf']
            edu_fieldofstudies_u2 = user_feature_dict[user2]['edu_idf']

            jobrole_intersections = set(jobroles_u1.keys()).intersection(jobroles_u2.keys())
            edu_intersections = set(edu_fieldofstudies_u1.keys()).intersection(edu_fieldofstudies_u2.keys())
            
            for jobrole in jobrole_intersections:
               num += jobroles_u1[jobrole]*jobroles_u2[jobrole]  
            for e in edu_intersections:
               num += edu_fieldofstudies_u1[e]*edu_fieldofstudies_u2[e]

            if career_level_u1  ==  career_level_u2:
               num += user_feature_dict[user1]['career_level_idf']*user_feature_dict[user2]['career_level_idf']
            if discipline_id_u1!=0 and discipline_id_u2!=0 and discipline_id_u1==discipline_id_u2:
               num += user_feature_dict[user1]['discipline_id_idf']*user_feature_dict[user2]['discipline_id_idf']
            if industry_id_u1!=0 and industry_id_u2!=0 and industry_id_u1==industry_id_u2:
               num += user_feature_dict[user1]['industry_id_idf']*user_feature_dict[user2]['industry_id_idf']
            if country_u1!=0 and country_u2!=0 and country_u1==country_u2:
               num += user_feature_dict[user1]['country_idf']*user_feature_dict[user2]['country_idf']
            if region_u1!=0 and region_u2!=0 and region_u1==region_u2:
               num += user_feature_dict[user1]['region_idf']*user_feature_dict[user2]['region_idf']
            if edu_degree_u1!=0 and edu_degree_u2!=0 and edu_degree_u1==edu_degree_u2:
               num += user_feature_dict[user1]['edu_degree_idf']*user_feature_dict[user2]['edu_degree_idf']

            norm_u1 = user_feature_dict[user1]['features_norm']
            norm_u2 = user_feature_dict[user2]['features_norm']
            if jaccard:
               return num/ (norm_u1 + norm_u2)
            else:
               return num/ (norm_u1 * norm_u2 +30.0)
#######################################################
#             RECOMMENDATION ALGORITHMS               #
#######################################################
def make_recommendation_with_collaborative_filtering_item_item(target , i , number_of_items , implicit , item_to_avoid , fall_back , jaccard):
      
      print "user number : ",i+1
      items_of_user = set()
      ratings_item_collaborative_filtering_item_item = {item: 0 for item in recommendable_items_for_collaborative}
      if target in user_items_dict:
         items_of_user = user_items_dict[target]
     
      items_to_rank = set()
      
      for item in items_of_user:
         users = item_users_dict[item]['implicit_rating'].keys()
         for user in users:
            items = user_items_dict[user]
            for u in items:
               items_to_rank.add(u)

      for item1 in items_of_user :
         for item2 in items_to_rank:
            if item2 in recommendable_items_for_collaborative and item2 not in items_of_user and item2 not in item_to_avoid:
                  if implicit:
                   ratings_item_collaborative_filtering_item_item[item2] += user_feature_dict[target]['explicit_rating'][item1]*collaborative_similarity_item_item_implicit(item1, item2 , jaccard)
                  else :
                   ratings_item_collaborative_filtering_item_item[item2] += user_feature_dict[target]['explicit_rating'][item1]*collaborative_similarity_item_item_no_implicit(item1, item2, jaccard)

      sorted_by_rating_item = sorted(ratings_item_collaborative_filtering_item_item, key=ratings_item_collaborative_filtering_item_item.get, reverse=True)
      
      
      for num,item in enumerate(sorted_by_rating_item[:number_of_items]):
         if ratings_item_collaborative_filtering_item_item[item]==0:
            if fall_back:
               print "CF-II SCORE = O  AT ITEM " + str(num) + "  : CHOOSE CF-UU ALGORITHM FOR USER :" + str(target)
               submission = make_recommendation_with_collaborative_filtering_user_user(target , i , 115 , number_of_items-num , True , item_to_avoid ,True , False)
               items = submission.split(',')[1]
               for new_item in items.split(' '):
                  item_to_avoid.append(toInt(new_item))

            items_with_rating = []
            for item in item_to_avoid[:number_of_items]:
               item_with_rating = str(item) + ';' + str(ratings_item_collaborative_filtering_item_item[item])
               items_with_rating.append(item_with_rating)

            suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
            submission = str(target) + ',' + suggestions + '\n'
            return submission
         else:
            item_to_avoid.append(item)

      items_with_rating = []
      for item in item_to_avoid[:number_of_items]:
         item_with_rating = str(item) + ';' + str(ratings_item_collaborative_filtering_item_item[item])
         items_with_rating.append(item_with_rating)

      suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
      submission = str(target) + ',' + suggestions + '\n' 
     
      return submission
def make_recommendation_with_collaborative_filtering_user_user(target , i ,number_of_neighbours, number_of_items , implicit , item_to_avoid, fall_back , jaccard):
   print "user number : ",i+1

   items_of_target = set()
   ratings_users_cf_u_u = {user: 0 for user in users_interactions}
   if target in user_items_dict:
      items_of_target = user_items_dict[target]
   
   for user in users_interactions:
      if user != target:
         if implicit:
          ratings_users_cf_u_u[user] = collaborative_similarity_user_user_implicit(user,target,jaccard)
         else:
          ratings_users_cf_u_u[user] = collaborative_similarity_user_user_no_implicit(user, target , jaccard)
   
   sorted_by_rating_u_u_s = sorted(ratings_users_cf_u_u, key=ratings_users_cf_u_u.get, reverse=True)
   sorted_by_rating_u_u_s = sorted_by_rating_u_u_s[:number_of_neighbours]
   
   ratings = {item: 0 for item in recommendable_items_for_collaborative}
   
   for user in sorted_by_rating_u_u_s:
      recommendable_set = set(user_items_dict[user]).intersection(recommendable_items_for_collaborative)
      for item in recommendable_set:
         if item not in items_of_target  and item not in item_to_avoid:
            ratings[item] += ratings_users_cf_u_u[user]
   
   sorted_by_rating = sorted(ratings, key=ratings.get, reverse=True)

   
   for num,item in enumerate(sorted_by_rating[:number_of_items]):
      if ratings[item]==0:
         if fall_back:
            print "CF-UU SCORE = O AT ITEM " + str(num) + "  : CHOOSE CB-II ALGORITHM FOR USER :" + str(target)
            submission = make_recommendation_with_content_based_item_item(target , i ,number_of_items- num , item_to_avoid ,True)
            items = submission.split(',')[1]
            for new_item in items.split(' '):
               item_to_avoid.append(toInt(new_item))

         items_with_rating = []
         for item in item_to_avoid[:number_of_items]:
            item_with_rating = str(item) + ';' + str(ratings[item])
            items_with_rating.append(item_with_rating)


         suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
         submission = str(target) + ',' + suggestions + '\n'
         return submission
      else:
         item_to_avoid.append(item)
  
   items_with_rating = []
   for item in item_to_avoid[:number_of_items]:
      item_with_rating = str(item) + ';' + str(ratings[item])
      items_with_rating.append(item_with_rating)

   suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
   submission = str(target) + ',' + suggestions + '\n' 

   return submission
def make_recommendation_with_content_based_item_item(target , i , number_of_items , item_to_avoid , fall_back ):
      
      print "user number : ",i+1

      items_of_user = set()

      if target in user_items_dict:
         items_of_user = user_items_dict[target]
      
      ratings_content_based_item_item = {item: 0 for item in recommendable_items}
      
      items_to_rank = set()

      for item1 in items_of_user:
         for user in item_users_dict[item1]['implicit_rating']:
            for item2 in user_items_dict[user]:
               items_to_rank.add(item2)

      if len(items_to_rank) < 99999999:
         items_to_rank = item_users_dict.keys()

      for item_to_recommend in items_to_rank:
         if item_to_recommend not in items_of_user and item_to_recommend in recommendable_items and item_to_recommend not in item_to_avoid:
            for item_of_user in items_of_user:               
               ratings_content_based_item_item[item_to_recommend] += user_feature_dict[target]['explicit_rating'][item_of_user]*content_similarity_item_item(item_to_recommend,item_of_user) 
      
      sorted_by_similarity_tf_idf = sorted(ratings_content_based_item_item, key=ratings_content_based_item_item.get, reverse=True)
         
      
      for num,item in enumerate(sorted_by_similarity_tf_idf[:number_of_items]):
         if ratings_content_based_item_item[item]==0:
            if fall_back:
               print "CB-II SCORE = O AT ITEM " + str(num) + "  : CHOOSE CB-UI ALGORITHM FOR USER :" + str(target)
               submission = make_recommendation_with_content_based_user_user(target , i , number_of_items- num  , item_to_avoid , True , False)
               items = submission.split(',')[1]
               for new_item in items.split(' '):
                  item_to_avoid.append(toInt(new_item))

            items_with_rating = []
            for item in item_to_avoid[:number_of_items]:
               item_with_rating = str(item) + ';' + str(ratings_content_based_item_item[item])
               items_with_rating.append(item_with_rating)

            suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
            submission = str(target) + ',' + suggestions + '\n'
            return submission
         else:
            item_to_avoid.append(item)
      
      items_with_rating = []
      for item in item_to_avoid[:number_of_items]:
         item_with_rating = str(item) + ';' + str(ratings_content_based_item_item[item])
         items_with_rating.append(item_with_rating)

      suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
      submission = str(target) + ',' + suggestions + '\n' 

      return submission
def make_recommendation_with_content_based_user_user(target , i , number_of_items , item_to_avoid , fall_back , jaccard):
      
      print "user number : ",i+1

      items_of_user = set()
      if target in user_items_dict:
         items_of_user = user_items_dict[target]

      ratings_user_user_content = {user: 0 for user in user_feature_dict}
      ratings = {item: 0 for item in recommendable_items}

      for user in user_feature_dict:
               if jaccard:
                  ratings_user_user_content[user] = content_similarity_user_user(user,target, True)
               else:
                  ratings_user_user_content[user] = content_similarity_user_user(user,target, False)
      
      sorted_users = sorted(ratings_user_user_content, key=ratings_user_user_content.get, reverse=True)
     
      sorted_users = sorted_users[:300]

      for neigh in sorted_users:
         if neigh in user_items_dict:
            for item in user_items_dict[neigh]:
                if item in rec and item not in item_to_avoid:
                     if item not in items_of_user:
                        ratings[item] += ratings_user_user_content[neigh]
      sorted_by_rating = sorted(ratings, key=ratings.get, reverse=True)

      items_with_rating = []
      for item in sorted_by_rating[:number_of_items]:
         item_with_rating = str(item) + ';' + str(ratings[item])
         items_with_rating.append(item_with_rating)

      suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
      submission = str(target) + ',' + suggestions + '\n'
      return submission
def make_recommendation_with_content_based_user_item(target , i , number_of_items , item_to_avoid  , fall_back , jaccard):
      
      print "user number : ",i+1

      items_of_user = set()

      if target in user_items_dict:
         items_of_user = user_items_dict[target]

      ratings_content_based_user_item = {item: 0 for item in recommendable_items}
      
      for item in item_users_dict.keys():
            if item not in items_of_user and item in recommendable_items:
               if jaccard:
                  ratings_content_based_user_item[item] = content_similarity_user_item(target,item , True)
               else:
                  ratings_content_based_user_item[item] = content_similarity_user_item(target,item , False)
      
      sorted_by_rating_content_based_user_item = sorted(ratings_content_based_user_item, key=ratings_content_based_user_item.get, reverse=True)
      
      for num,item in enumerate(sorted_by_rating_content_based_user_item[:number_of_items]):
         if ratings_content_based_user_item[item]==0:
            if fall_back:
               print "CB-UI SCORE = O AT ITEM " + str(num) + "  : CHOOSE CB-UU ALGORITHM FOR USER :" + str(target)
               submission = make_recommendation_with_content_based_user_user(target , i , number_of_items- num , item_to_avoid , True , False)
               items = submission.split(',')[1]
               for new_item in items.split(' '):
                  item_to_avoid.append(toInt(new_item))
            items_with_rating = []
            for item in item_to_avoid:
               item_with_rating = str(item) + ';' + str(ratings_content_based_user_item[item])
               items_with_rating.append(item_with_rating)
            suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
            submission = str(target) + ',' + suggestions + '\n' 
            return submission
         else:
            item_to_avoid.append(item)

      items_with_rating = []
      for item in item_to_avoid:
         item_with_rating = str(item) + ';' + str(ratings_content_based_user_item[item])
         items_with_rating.append(item_with_rating)

      suggestions = str(' '.join(map(str, items_with_rating[:number_of_items])))
      submission = str(target) + ',' + suggestions + '\n' 

      return submission
#######################################################
#                       MAIN                          #
#######################################################

title_items_dict = defaultdict(set)
item_feature_dict = b_items(items)
user_feature_dict = b_users(users)

print "build_users_dicts_and_sets..."
target_users = b_target_users(targets)
users_interactions = users_interactions(interactions)
user_items_dict = user_items_dict(interactions)    

item_frequency_dict = {item : 0 for item in item_feature_dict.keys()}  

print "build_items_dicts_and_sets..."
item_time_dict , max_inter = b_latest_interaction(interactions , 0)
item_users_list = b_item_users_list(interactions)
item_users_dict = b_item_user_dict(item_users_list)
recommendable_items , recommendable_items_for_collaborative , rec= b_recommendable_items()


print "build_iteractions..."
b_interactions()

print "build_item_and_user_profile..."
b_u_i_profile()

print "split_known_and_unknown_users..."
for user in target_users:
    u = user_items_dict[user]
    if len(u) != 0 :
        known_users.add(user)
    else:
        unknown_users.add(user)

print "build_item_features_for_collaborative"
b_item_features_dict(interactions)

print "user_idf and item_idf_norm"
user_idf , item_idf_norm , user_idf_norm = b_idf_users_interactions()

#######################################################
#                   RECOMMEND                         #
#######################################################


u_cb_uu = (Parallel(n_jobs=-1)(delayed(final_ensemble)(target, i, 5 ) for i,target in enumerate(known_users)))


CF_II_NO_IMPLICIT_NO_FALLBACK = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_collaborative_filtering_item_item)(target , i , 300 , False , [] , False , False) for i,target in enumerate(target_users)))

print "open_file"
filename = 'CF_II_NO_IMPLICIT_NO_FALLBACK.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CF_II_NO_IMPLICIT_NO_FALLBACK:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)

CF_II_IMPLICIT_NO_FALLBACK = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_collaborative_filtering_item_item)(target , i , 300 , True , [] , False ,  False) for i,target in enumerate(target_users)))

print "open_file"
filename = 'CF_II_IMPLICIT_IDF_USERS_NO_FALLBACK.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CF_II_IMPLICIT_NO_FALLBACK:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)


CF_II_NO_IMPLICIT_NO_FALLBACK_JACCARD = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_collaborative_filtering_item_item)(target , i , 300 , False , [] , False , True) for i,target in enumerate(target_users)))

print "open_file"
filename = 'CF_II_NO_IMPLICIT_NO_FALLBACK_JACCARD.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CF_II_NO_IMPLICIT_NO_FALLBACK_JACCARD:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)

CF_II_IMPLICIT_NO_FALLBACK_JACCARD = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_collaborative_filtering_item_item)(target , i , 300 , True , [] , False, True) for i,target in enumerate(target_users)))

print "open_file"
filename = 'CF_II_IMPLICIT_IDF_USERS_NO_FALLBACK_JACCARD.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CF_II_IMPLICIT_NO_FALLBACK_JACCARD:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)


CF_UU_NO_IMPLICIT_NO_FALLBACK = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_collaborative_filtering_user_user)(target , i ,115 ,  300 , False , [] , False, False)for i,target in enumerate(target_users)))

print "open_file"
filename = 'CF_UU_NO_IMPLICIT_NO_FALLBACK.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CF_UU_NO_IMPLICIT_NO_FALLBACK:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)



CF_UU_IMPLICIT_NO_FALLBACK = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_collaborative_filtering_user_user)(target , i ,115 ,  300 , True , [] , False, False)for i,target in enumerate(target_users)))

print "open_file"
filename = 'CF_UU_IMPLICIT_IDF_USERS_NO_FALLBACK.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CF_UU_IMPLICIT_NO_FALLBACK:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)

CF_UU_NO_IMPLICIT_NO_FALLBACK_JACCARD = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_collaborative_filtering_user_user)(target , i ,115 ,  300 , False , [] , False, True)for i,target in enumerate(target_users)))

print "open_file"
filename = 'CF_UU_NO_IMPLICIT_NO_FALLBACK_JACCARD.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CF_UU_NO_IMPLICIT_NO_FALLBACK_JACCARD:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)




CF_UU_IMPLICIT_NO_FALLBACK_JACCARD = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_collaborative_filtering_user_user)(target , i ,115 ,  300 , True , [] , False, True)for i,target in enumerate(target_users)))

print "open_file"
filename = 'CF_UU_IMPLICIT_IDF_USERS_NO_FALLBACK_JACCARD.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CF_UU_IMPLICIT_NO_FALLBACK_JACCARD:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)


CB_UU = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_content_based_user_user)(target , i , 300, [] , False , False)for i,target in enumerate(target_users)))
print "open_file"
filename = 'CB_UU.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CB_UU:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)



CB_II_NO_FALLBACK = (Parallel(n_jobs=-1)(delayed(make_recommendation_with_content_based_item_item)(target, i , 300, [] , False)for i,target in enumerate(target_users)))
print "open_file"
filename = 'CB_II_NO_FALLBACK_JACCARD_NO_CUT.csv'
f2 = open(filename, 'w')
f2.write('user_id,recommended_items\n')
for sub in CB_II_NO_FALLBACK:
      if len(sub.split(',')[1].split(' '))!=300:
       print "WARNING LEN IN SUB NOT CORRESPOND "
      f2.write(sub)


