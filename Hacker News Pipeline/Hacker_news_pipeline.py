#!/usr/bin/env python
# coding: utf-8

# TODOs:
# 
# - Rewrite the Pipeline class' output to save a file of the output for each task. This will allow you to "checkpoint" tasks so they don't have to be run twice.
# - Use the nltk package for more advanced natural language processing tasks.
# - Convert to a CSV before filtering, so you can keep all the stories from 2014 in a raw file.
# - Fetch the data from Hacker News directly from a JSON API. Instead of reading from the file we gave, you can perform additional data processing using newer data.

# In[1]:


import json
from pipeline import Pipeline, build_csv
from datetime import datetime
import io
import csv
import string
from stop_words import stop_words
import operator

pipeline = Pipeline()

@pipeline.task()
def file_to_json():
    
    f = open('hn_stories_2014.json', 'r')
    
    # Load json file into a Python dictionary
    data = json.load(f) # file to string
    stories = data['stories'] 
    
    return stories

@pipeline.task(depends_on = file_to_json)
def filter_stories(stories):
    # Return a generator function to avoid memory overflow
    def is_popular(story):
        return story['points'] > 50 and story['num_comments'] > 1 and not story['title'].startswith('Ask HN')     
    return (
        story for story in stories
        if is_popular(story)
    )  

@pipeline.task(depends_on = filter_stories)
def json_to_csv(stories):

    lines = []
    for story in stories:
        objectID = story['objectID']
        created_at = datetime.strptime(story['created_at'], "%Y-%m-%dT%H:%M:%SZ")
        url = story['url']
        points = story['points']
        title = story['title']
        
        lines.append((objectID, created_at, url, points, title))
    
    header = ['objectID', 'created_at', 'url', 'points', 'title']
    
    return build_csv(lines, header, file=io.StringIO())

@pipeline.task(depends_on = json_to_csv)
def extract_titles(csv_file):
    def inner():
        #f = open(csv_file, 'r')
        csv_reader = csv.reader(csv_file)
        
        headers = next(csv_reader)
        idx = headers.index('title')
        return (
            l[idx] for l in csv_reader
        )
    return inner()


@pipeline.task(depends_on = extract_titles)
def clean_titles(titles):
    
    def remove_punctuation_and_lower_title():
        
        # Need to review maketrans and translate methods
        str_to_remove = string.punctuation + '‘' + '’'
        remove = str.maketrans({key: None for key in str_to_remove})
        return (
            t.translate(remove).lower() for t in titles
        )
    return remove_punctuation_and_lower_title()

@pipeline.task(depends_on = clean_titles)
def build_keyword_dictionary(clean_titles):
    
    word_count = {}
    
    for t in clean_titles:
        words = t.split()
        for w in words:
            if w not in stop_words:
                if w not in word_count.keys():
                    word_count[w] = 0 
                word_count[w] += 1
    return word_count

@pipeline.task(depends_on = build_keyword_dictionary)
def top_100_words_used_in_titles(word_count):
    
    # sorted(word_count, key=word_count.get, reverse=True)
    sorted_words = sorted(word_count.items(), key=operator.itemgetter(1), 
                          reverse = True)
    return sorted_words[:100]
    
            
output = pipeline.run()
print(output[top_100_words_used_in_titles])
print() 


# The code below is for test purposes:

# In[4]:


def file_to_json():
    
    f = open('hn_stories_2014.json', 'r')
    
    # Load json file into a Python dictionary
    data = json.load(f) # file to string
    stories = data['stories'] 
    
    return stories

def filter_stories(stories):
    # Return a generator function to avoid memory overflow
    def is_popular(story):
        return story['points'] > 50 and story['num_comments'] > 1 and not story['title'].startswith('Ask HN')     
    return (
        story for story in stories
        if is_popular(story)
    )  

def json_to_csv(stories):

    lines = []
    for story in stories:
        objectID = story['objectID']
        created_at = datetime.strptime(story['created_at'], "%Y-%m-%dT%H:%M:%SZ")
        url = story['url']
        points = story['points']
        title = story['title']
        
        lines.append((objectID, created_at, url, points, title))
    
    header = ['objectID', 'created_at', 'url', 'points', 'title']
    
    return build_csv(lines, header, file=io.StringIO())

def extract_titles(csv_file):
    def inner():
        #f = open(csv_file, 'r')
        csv_reader = csv.reader(csv_file)
        
        headers = next(csv_reader)
        idx = headers.index('title')
        return (
            l[idx] for l in csv_reader
        )
    return inner()

def clean_titles(titles):
    
    def remove_punctuation_and_lower_title():
        
        # Need to review maketrans and translate methods
        str_to_remove = string.punctuation + '‘' + '’'
        remove = str.maketrans({key: None for key in str_to_remove})
        return (
            t.translate(remove).lower() for t in titles
        )
    return remove_punctuation_and_lower_title()

def build_keyword_dictionary(clean_titles):
    
    word_count = {}
    
    for t in clean_titles:
        words = t.split()
        for w in words:
            if w not in stop_words:
                if w not in word_count.keys():
                    word_count[w] = 0 
                word_count[w] += 1
    return word_count

def top_100_words_used_in_titles(word_count):
    sorted_words = sorted(word_count.items(), key=operator.itemgetter(1), 
                          reverse = True)
    
    print(type(sorted_words))
    
    return sorted_words[:10]
    
    
    
file = file_to_json()
stories = filter_stories(file)
csv_file = json_to_csv(stories)

titles = extract_titles(csv_file)
clean_titles = clean_titles(titles)
word_count = build_keyword_dictionary(clean_titles)
sorted_words = top_100_words_used_in_titles(word_count)

print(sorted_words)

for t in clean_titles:
    print(t)
    break

# True Goodbye: ‘Using TrueCrypt Is Not Secure’


# In[5]:


from datetime import datetime
created_at = '2014-05-29T04:27:42Z'

dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
print(dt)


# In[6]:


print((test['stories'][0]))

s = test['stories']

print(type(s))


# In[ ]:





# In[26]:


#print(stop_words)

if 'the' in stop_words:
    print('yep')


# In[ ]:




