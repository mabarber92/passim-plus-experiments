from utilities.load_all_cls import load_all_cls
import pandas as pd
import re
import os

"""Note this has been refactored to allow easier date filtering when fetching book and ms specific clusters using the fetch_df function
larger refactor of this code is needed to adopt a pipeline type approach (build a series of cluster filters and then apply them would be more flexible)"""

class clusterDf():
    def __init__ (self, cluster_path, meta_path, min_date=0, max_date = 1500, cluster_cap = 500, drop_strings = True, columns = ["uid", "gid", "cluster", "size", "seq", "series", "text", "begin", "end"]):
        self.cluster_df = load_all_cls(cluster_path, meta_path, drop_strings=drop_strings, columns = columns, drop_dates=False, max_date = max_date, min_date=min_date, cluster_cap = cluster_cap)
        self.cluster_df = self.clean_single_clusters(self.cluster_df)
        self.print_aggregated_stats()
        

    def clean_single_clusters(self, cl_df):
        """Filtering steps leave lone clusters - e.g. cluster of size 2 with a text from 845 and post 845
          filtered by date 845 will be left with only one item in the cluster. This creates problems downstream
          All filtering processes need to be passed to this function"""
        print("Cleaning up the single clusters")
        return cl_df.groupby("cluster").filter(lambda x: len(x) > 1)
        # new_cl_df = pd.DataFrame()
        # cluster_list = cl_df["cluster"].drop_duplicates().to_list()
        # for cluster in tqdm(cluster_list):
        #     filtered_df = cl_df[cl_df["cluster"] == cluster]
        #     if len(filtered_df) > 1:
        #         new_cl_df = pd.concat([new_cl_df, filtered_df])
        # return new_cl_df

    def count_books(self):
        return len(self.cluster_df[self.cluster_df["series"]].drop_duplicates())

    def count_clusters(self, df_in = None):
        if df_in is not None:
            return len(df_in["cluster"].drop_duplicates())
        else:
            return len(self.cluster_df["cluster"].drop_duplicates())
        
    def fetch_max_cluster(self):
        """Return a dataframe containing the largest cluster - WARNING for post processing this is a df containing all of the 
        rows of the cluster - it will need to be reduced to a single row for any aggregate stats on the cluster"""
        return self.cluster_df[self.cluster_df["size"] == self.cluster_df["size"].max()]

    def fetch_top_reusers(self, uri, uri_field="book", by = "length", exclude_self_reuse = False, dir = "bi", csv_out=None):
        # Set up pre-requisites to be used by other funcs
        self.exclude_self_reuse = exclude_self_reuse
        
        # Find death date of author and determine whether to filter before or after
        if dir != "bi":
            uri_death_date = int(re.findall("\d+", uri)[0])
            print(uri_death_date)
            if dir == "anachron":
                df_in = self.cluster_df[self.cluster_df["date"] < uri_death_date]
            elif dir == "chron":
                df_in = self.cluster_df[self.cluster_df["date"] > uri_death_date]
        else:
            df_in = self.cluster_df
        
        # Send filtered df to the calcuate function
        stats_df = self.calculate_reuse_stats(uri, uri_field=uri_field, df_in = df_in) 

        # Sort and return df
        stats_df = stats_df.sort_values(by=by, ascending=False)

        if csv_out:
            stats_df.to_csv(csv_out, index=False)
        
        return stats_df

    # Use a URI to fetch a list of clusters
    def fetch_clusters_by_uri(self, uri, uri_field = "book"):       

        cluster_df = self.cluster_df

        return cluster_df[cluster_df[uri_field] == uri]["cluster"].to_list()
    
    # Use a URI and a ms_list to fetch cluster list
    def fetch_clusters_by_uri_mslist(self, uri, ms_list, uri_field="book"):

        cluster_df = self.cluster_df

        filtered = cluster_df[cluster_df[uri_field] == uri]
        return filtered[filtered["seq"].isin(ms_list)]["cluster"].to_list()

    # Concatenate the uris in a cluster set
    def calculate_reuse_stats(self, uri, uri_field="book", df_in = None):
        cluster_list = self.fetch_clusters_by_uri(uri, uri_field=uri_field)        
        
        if df_in is None:
            df_in = self.cluster_df
        
        df_in = df_in[df_in["cluster"].isin(cluster_list)]    
        print(df_in)
        if self.exclude_self_reuse:
            
            df_in["author"] = df_in["book"].str.split(".", expand=True)[0]
            uri_author = uri.split(".")[0]
            df_in = df_in[df_in["author"] != uri_author]
        
        uri_list = df_in["book"].drop_duplicates().to_list()
        if uri in uri_list:
            uri_list.remove(uri)
        stat_dicts = []
        for uri in uri_list:
            uri_df = df_in[df_in["book"] == uri]
            uri_df["length"] = uri_df["end"] - uri_df["begin"]
            stat_dicts.append({"uri": uri, "length": uri_df["length"].sum(), "instances": len(uri_df)})
        
        return pd.DataFrame(stat_dicts)

    # Function to apply a date filter to the df
    def filter_by_date_range(self, min_date = 0, max_date= 1500, df_in=None, return_df=False):
        """Needs more careful refactoring, as there's no reason to filter self.cluster_df if an input df has been given. The default
        behaviour when df_in does not equal none would be to return a df"""
        if df_in is not None:
            cluster_df = df_in[df_in["date"].le(max_date)]
            cluster_df = cluster_df[cluster_df["date"].ge(min_date)]
            cluster_df = self.clean_single_clusters(cluster_df)
            return cluster_df
        else:
            if return_df:
                cluster_df = self.cluster_df[self.cluster_df["date"].le(max_date)]
                cluster_df = cluster_df[cluster_df["date"].ge(min_date)]
                cluster_df = self.clean_single_clusters(cluster_df)
                return cluster_df
            else:
                self.cluster_df = self.cluster_df[self.cluster_df["date"].le(max_date)]
                self.cluster_df = self.cluster_df[self.cluster_df["date"].ge(min_date)]
                self.cluster_df = self.clean_single_clusters(self.cluster_df)

    def filter_by_author_list(self, author_list):
        print("Filtering clusters by authors: {}".format(author_list))
        author_df = self.cluster_df.copy()
        author_df["author"] = author_df["book"].str.split(".", expand=True)[0]        
        self.cluster_df = author_df[author_df["author"].isin(author_list)]
        self.cluster_df = self.clean_single_clusters(self.cluster_df)
        self.cluster_df.drop(columns=["author"])
    
    def filter_by_book_list(self, book_list, exclude_listed_books=False):
        """If exclude_listed_books is true - it will return the only rows that do not match the book list"""
        if exclude_listed_books:
            print("Filtering clusters to exclude books: {}".format(book_list))
            self.cluster_df = self.cluster_df[~self.cluster_df["book"].isin(book_list)]
        else:
            print("Filtering clusters by books: {}".format(book_list))
            self.cluster_df = self.cluster_df[self.cluster_df["book"].isin(book_list)]
        self.cluster_df = self.clean_single_clusters(self.cluster_df)

    def return_cluster_df_for_uri_ms(self, primary_book, ms = None, input_type = "range", min_date = None, max_date = None):
        # None type allows this function to be used to fetch all of the clusters for an entire text (rather than specified milestones)
        if ms == None:
            clusters = self.fetch_clusters_by_uri(primary_book)
        else:
            if type(ms) == list and input_type == "range":
                if len(ms) == 2:
                    ms_list = list(range(ms[0], ms[1]+1))
                    print(ms_list)
                elif len(ms) == 1:
                    ms_list = ms[:]
                else:
                    print("Range specified but list is greater than two - for a range supply only start and end ms... treating input ms as list of ms")
                    input_type = "list"
            elif input_type == "list":
                ms_list = ms[:]
            else:
                ms_list = [ms]
                
            clusters = self.fetch_clusters_by_uri_mslist(primary_book, ms_list)

        cluster_df = self.cluster_df[self.cluster_df["cluster"].isin(clusters)]
        if min_date is not None and max_date is not None:
            cluster_df = self.filter_by_date_range(min_date=min_date, max_date=max_date, df_in=cluster_df)
        return cluster_df
    
    def print_aggregated_stats(self, greater_than_measure = 100):
        # Perform calculations
        cluster_count = self.count_clusters()

        clusters_greater_than = self.cluster_df[self.cluster_df["size"] > greater_than_measure]
        count_clusters_greater_than = self.count_clusters(df_in = clusters_greater_than)

        largest_cluster = self.fetch_max_cluster().iloc[0]

        # Print results
        print("Total number of clusters: {}".format(cluster_count))
        print("Total number of clusters with a size greater than {} : {}".format(greater_than_measure, count_clusters_greater_than))
        print("Size of largest cluster (cluster {}): {}".format(largest_cluster["cluster"], largest_cluster["size"]))


    def to_minified_csv(self, out_path, columns = ["cluster", "id", "seq", "begin", "end", "size"]):
        minified_csv = self.cluster_df[columns]
        minified_csv.to_csv(out_path)

if __name__ == "__main__":
    print(os.getcwd())
    clusters = "D:/Corpus Stats/2023/v8-clusters/out.parquet"
    meta = "D:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
#     out_csv = "D:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
#     cluster_df_obj = clusterDf(clusters, meta, max_date = 1000, cluster_cap=500)    
#     cluster_df_obj.to_minified_csv(out_csv)
