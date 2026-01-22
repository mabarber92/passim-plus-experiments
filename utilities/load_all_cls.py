# -*- coding: utf-8 -*-
"""
Created on Fri May 27 14:38:58 2022

@author: mathe
"""
import pandas as pd
import pyarrow.parquet as pq
import os
from tqdm import tqdm

def load_all_cls(path, meta_path, min_date=1, max_date = 900, cluster_cap = 500, columns = ["uid", "gid", "cluster", "size", "seq", "series", "text", "begin", "end"], drop_strings = False, drop_dates = True):
    
    meta_df = pd.read_csv(meta_path, sep="\t")[["id", "book", "date"]]
    
    if path.split(".")[-1] == "csv":
        print("Loading Minified Clusters")
        all_cls = pd.read_csv(path)
        all_cls = pd.merge(all_cls, meta_df, on="id")
        if cluster_cap is not None:
            all_cls = all_cls[all_cls["size"] < cluster_cap]
        all_cls = all_cls[all_cls["date"].ge(min_date)]
        all_cls = all_cls[all_cls["date"].le(max_date)]
    else:
        all_cls = pd.DataFrame()
        if "size" not in columns:
            columns.append("size")
        if "series" not in columns:
            columns.append("series")
        print("Loading all clusters below: " + str(cluster_cap))
        print(path)
        
        for root, dirs, files in os.walk(path, topdown=False):
            for name in tqdm(files):
                file_type = None
                split_name = name.split(".")
                if split_name[-1] == "crc":
                    continue
                if split_name[-1] == "parquet":
                    file_type = "parquet"
                if split_name[-1] == "json":
                    file_type = "json"
                if file_type == None:
                    print("Unrecognised file format. File name: {} ... skipping to next file".format(name))
                    continue  
                if file_type == "parquet" or file_type == "json":
                    file_path = os.path.join(root, name)
                    if drop_strings:
                        if "text" in columns:
                            columns.remove("text")
                    else:
                        if "text" not in columns:
                            columns.append("text")
                    
                            
                    if file_type == "json":
                        data = pd.read_json(file_path, lines=True)[columns]
                    else:
                        data = pq.read_table(file_path).to_pandas()[columns]

                    if cluster_cap is not None:
                        data = data[data["size"] < cluster_cap]

                    data["id"] = data["series"].str.split("-").str[0]
                    data = pd.merge(data, meta_df, how = "inner", on ="id") 
                    # Applying the date filter to output                
                    data = data[data["date"].le(max_date)]
                    data = data[data["date"].ge(min_date)]
                    
                    if drop_dates:
                        data= data.drop(columns = ["date"])

                    all_cls = pd.concat([all_cls, data])
        
    

    


    print("New cluster data loaded...")
    
    
    
    return all_cls

