from utilities.clusterDf import clusterDf
from utilities.openitiTexts import openitiTextMs
import json
import re
import pandas as pd
import os
from tqdm import tqdm

def check_gap(prev_dict, next_dict, min_gap, av_word_len=4):
    """
    Take dict data from cluster rows and compare them to see if they meet the match criteria
    prev_dict: dict of the first row (before the hypothesised gap) from the cluster data
    next_dict: dict of the next row (after the hypothesised gap) from the cluster data
    min_gap: the minimum gap size to meet criteria
    av_word_len: average word length in corpus in ch, used to create a theoretical length of the milestones in chs - not a precise measurement

    Returns:
    bool: true means there is a gap, false means the gap does not meet criteria
    """
    gap = False
    ms_gap = next_dict["seq"] - prev_dict["seq"]
    # If the ms (seq) is the same, calculate gap between 'end' of book_ms and 'begin' of next_ms
    if ms_gap == 0:
        gap_len = next_dict["begin"] - prev_dict["end"]
        if gap_len > min_gap:
            gap = True
   
    # If the ms are consecutive gap take notional ch length of ms (4*300) - 'end' of book_ms, if negative number use 0 + "begin" of following ms
    elif ms_gap == 1:
        prev_gap = (av_word_len * 300) - prev_dict["end"]
        if prev_gap < 0:
            prev_gap = 0
        next_gap = next_dict["begin"]
        gap_len = next_gap - prev_gap
        if gap_len > min_gap:
            gap = True

    return gap

def create_gap_dict(prev_dict, next_dict):
    """Take a record of previous and next cluster data and convert it into a dictionary that documents the gap
    Returns dict like:
    {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}}"""
    prev_uri = prev_dict["book"]
    next_uri = next_dict["book"]
    if prev_uri != next_uri:
        print(f"{prev_uri} and {next_uri} do not match - supply shared uris to create a gap dictionary")
        exit()
    else:
        start = {"ms": prev_dict["seq"], "ch": prev_dict["end"]}
        end = {"ms": next_dict["seq"], "ch": next_dict["begin"]}

        return {"book": prev_uri, "start": start, "end": end}

def query_book(cluster_obj, book_uri, min_gap=12, index_start = 0, data_check=False):
    """Take one book URI and fetch gaps as dict of aligned gaps
    In:
    book_uri: a book uri which is the base text for comparison, version URI not needed
    cluster_obj: cluster object produced by the clusterDF class
    min_gap: the minimum gap in characters between two reuse instances for it to be considered an alignment
    index_start: the first index of the output dict. Used when running a whole corpus to ensure that all identifiers are unique
    data_check: if you want to check the results against the input data, set this to true and it will return all of the rows of the
    cluster data that were used to support a result
    Returns: type dict
    [
        {"index": 1,
        "gaps_data": 
            [{"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}},
            {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}},
            {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}}]
        }
            ,
        {"index": 2,
        "gaps_data": 
            [{"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}},
            {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}},
           {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}}]
            }
        }
    ]
    To do: once this code is working - pass a ms text dict to this function and use it to get real ms lens in char - pass the full
    ms text to the output to allow us to use it in the next processing steps (avoid high IO ops)
    """

    # Create empty list for adding data
    out_data = []

    # Fetch a df of the clusters for the given book_uri - only for books dating before the book_uri death date
    death_date = int(re.findall("\d+", book_uri)[0])
    book_clusters = cluster_obj.return_cluster_df_for_uri_ms(book_uri, min_date=0, max_date=death_date)


    # Get the milestones for the clusters in the main book
    book_dict = book_clusters[book_clusters["book"] == book_uri].sort_values(by= ["seq", "begin"]).to_dict("records")

    # Advance through book_dict, one-by-one, calculate gap, if meets min_gap, see if there are shared books in the pair, then find gaps for those
    for idx, current_row in enumerate(tqdm(book_dict[:-1])):
        
        # Get the next row
        next_row = book_dict[idx + 1]

        # Check the gap
        gap = check_gap(current_row, next_row, min_gap)

        if gap:
            book_list = []
            # If gap criteria fit check for matching gaps on other side of relationship
            matching_gaps = []
            # Get cluster dfs for either side of relationship
            reuse_before = book_clusters[book_clusters["cluster"] == current_row["cluster"]] 
            reuse_after = book_clusters[book_clusters["cluster"] == next_row["cluster"]] 

            # Get matching books
            books_before = reuse_before["book"].drop_duplicates().to_list()
            books_after = reuse_after["book"].drop_duplicates().to_list()
            matching_books = []
            for book in books_before:
                if book in books_after and book != book_uri:
                    matching_books.append(book)

            # For matching books see if gap condition is met - store cases where it is
            for matching_book in matching_books:
                dicts_before = reuse_before[reuse_before["book"] == matching_book].to_dict("records")
                dicts_after = reuse_after[reuse_after["book"] == matching_book].to_dict("records")
                for before_dict in dicts_before:
                    for after_dict in dicts_after:
                        matching_gap = check_gap(before_dict, after_dict, min_gap=min_gap)
                        if matching_gap:
                            gap_dict = create_gap_dict(before_dict, after_dict)
                            matching_gaps.append(gap_dict)
                            book_list.append(matching_book)
            
            if len(matching_gaps) > 0:
                index_start +=1
                main_dict = create_gap_dict(current_row, next_row)
                book_list.append(book_uri)
                if data_check:
                    out_dict = {
                            "index": index_start,
                            "gaps_data": [main_dict] + matching_gaps,
                            "books": book_list,
                            "supporting_data": {
                                "before": reuse_before.to_dict("records"),
                                "after": reuse_after.to_dict("records")}
                            }
                else:
                    out_dict = {
                        "index": index_start,
                        "gaps_data": [main_dict] + matching_gaps,
                        "books": book_list
                        }
                out_data.append(out_dict)

    # Return the results
    return out_data

def create_path_dict(meta_path, openiti_base_dir):
    """Create a dictionary where keys are books and the path links to specified openit_base_dir"""

    meta_df = pd.read_csv(meta_path, sep="\t")
    meta_df = meta_df[meta_df["status"]=="pri"]

    meta_dict = meta_df[["book", "local_path"]].to_dict("records")
    out_dict = {}

    for meta in meta_dict:
        full_path = os.path.join(openiti_base_dir, meta["local_path"].split("../")[-1])
        out_dict[meta["book"]] = full_path
    
    return out_dict



def populate_offset_text(gap_data, path_dict):
    """Take gap data and add text field by parsing the relevant openiti texts"""
    
    # Get list of all book names in gap_data
    print("Getting book names from data")
    book_list = []
    for row in tqdm(gap_data):
        book_data = row["gaps_data"]
        for dict_item in book_data:
            book = dict_item["book"]
            if book not in book_list:
                book_list.append(book)
    
    # Loop through each book - for each book loop through the data and populate the text according to specified offsets
    for book in tqdm(book_list):
        openiti_path = path_dict[book]
        ms_obj = openitiTextMs(openiti_path)

        for row in gap_data:
            if book in row["books"]:
                for gap in row["gaps_data"]:
                    if gap["book"] == book:
                        ms_start = gap["start"]["ms"]
                        ms_end = gap["end"]["ms"]
                        if ms_end - ms_start == 0:
                            text = ms_obj.fetch_offset_clean(ms_start, start= gap["start"]["ch"], end = gap["end"]["ch"])
                        else:
                            text = ms_obj.fetch_ms_list_clean([ms_start, ms_end], start=gap["start"]["ch"], end = gap["end"]["ch"])
                        
                        # Add text to the data
                        gap["text"] = text

    # Return updated data
    return gap_data


def query_corpus(cluster_obj, book_list = []):
    """

    In:
    cluster_obj: cluster object produced by the clusterDF class
    book_list: a list of book_uris to use, if empty run whole corpus, if one book only data for one book

    """


def run_pipeline(cluster_path, meta_path, openiti_base_dir, book_list = [], raw_gaps_out=None):
    """Run full processing pipeline from cluster data to data about gaps
    In:
    cluster_path: path to the cluster data (csv, json dir or parquet dir)
    meta_path: path to metadata, used by clusterDF
    book_list: a list of book_uris to use, if empty run whole corpus, if one book only data for one book
    raw_gaps_out: a path to export a raw gaps json (produced by query_book or query_corpus)
    """

    # Create the cluster object
    cluster_obj = clusterDf(cluster_path, meta_path)

    # If we only have one book, just run query book
    book_count = len(book_list)
    if book_count == 1:
        gap_data = query_book(cluster_obj, book_list[0])
    
    else:
        gap_data = query_corpus(cluster_obj, book_list)
    
    # Use corpus to fetch text

    # Produce dict of file paths for books
    path_dict = create_path_dict(meta_path, openiti_base_dir)

    # Add offsetted text pieces to the gap_data
    gap_data = populate_offset_text(gap_data, path_dict)
    
    
    # Export a json of the gap_data if the path is given
    if raw_gaps_out:
        json_string = json.dumps(gap_data, ensure_ascii=False, indent=4)
        with open(raw_gaps_out, "w", encoding='utf-8') as f:
            f.write(json_string)
        
    # 

    # and produce pairwise files

