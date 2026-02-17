"""Python classes for handling pairs of text and making comparisons. For example a 
diff or a tfidf similarity"""

import os
import sys

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the parent directory to 
# the sys.path.
sys.path.append(parent)

from py_kitab_diff import kitab_diff
from utilities.data_parsing import gapsClusters


gaps_obj = gapsClusters("find_shared_gaps/0421Miskawayh.json")
sample_data_row = gaps_obj.gaps_dict[0]["gaps_data"]

text1 = sample_data_row[0]["text"]
text2 = sample_data_row[1]["text"]

text1, text2, data1, data2 = kitab_diff(text1, text2)

pairs = []
unmatched = []
matches = []

def id_text_dict(dict_list):
    id_dict = {}
    for result in dict_list:
        
        if result["type"] == "=":
            id_dict[result["id"]] = result["text"]
    return id_dict

sim1 = id_text_dict(data1)
sim2 = id_text_dict(data1)

for id in sim1.keys():
    if id in sim2.keys():
        pairs.append([sim1[id], sim2[id]])
        matches.append(id)
    else:
        unmatched.append(id)

for id in sim2.keys():
    if id not in matches:
        unmatched.append(id)
            
print(pairs)
print(unmatched)
