"""Classes used for storing, processing and converting data types used across pipelines
for easy conversion to csv or LabelStudio compliant data"""
import json
import os
import pandas as pd

class gapsClusters():
    """Take list of dictionaries formated like this and render it as a series of formats:
     [{"index": 1,
        "gaps_data": 
            [{"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}, "text": "..."},
            {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}, "text": "..."},
            {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}, "text": "..."}]
        }
            ,
        {"index": 2,
        "gaps_data": 
            [{"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}, "text": "..."},
            {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}, "text": "..."},
           {"book": "0000AuthorBook", "start": {"ms": 1, "ch": 200}, "end": {"ms": 1, "ch": 300}, "text": "..."}]
            }
        }
        ]"""
    def __init__(self, gaps_data):
        """Load from either json or take a gaps_dict directly. Check that the data conforms to format - if so assign it"""
        
        # If the input is a str check it and load it
        if type(gaps_data) == str:
            if gaps_data.split(".")[-1] == ".json":
                gaps_data = self.load_json(gaps_data)
            else:
                print(f"invalid file path: {gaps_data}")
                exit()

        # Check the data structure
        self.check_data_dict(gaps_data)

        # Store the data dict
        self.gaps_dict = gaps_data

    
    def check_data_dict(self, gaps_dict):

        """Quite naive - just exit when we find the first error - we could write a log of errors if useful"""
        for row in gaps_dict:
            if "index" not in row.keys():
                print("Error found in index key")
                exit()
            if "gaps_data" in row.keys():
                gaps_data = row["gaps_data"]
                for gap in gaps_data:
                    if "book" not in gap.keys():
                        print("Error found in 'book' key")
                        exit()
                    if "start" in gap.keys():
                        start = gap["start"]
                        if "ms" not in start.keys():
                            print("Error found in ms key for start")
                            exit()
                        if "ch" not in start.keys():
                            print("Error found in ch key for start ")
                            exit()
                    else:
                        exit()
                    if "end" in gap.keys():
                        end = gap["end"]
                        if "ms" not in end.keys():
                            print("Error found in ms key for end")
                            exit()
                        if "ch" not in end.keys():
                            print("Error found in ch key for end")
                            exit()
                    else:
                        exit()
                    if "text" not in gap.keys():
                        print("Error found in text key")
                        exit()
            else:
                print("Error found in 'gaps_data' key")
                exit()
            if "books" not in row.keys():
                print("Error found in books key")
                exit()
            if "text_after" in row.keys() and "text_before" in row.keys():
                 self.surround_text = True
            else:
                 self.surround_text = False

    def load_json(self, json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.loads(f)
        return data
    
    def save_json(self, export_path):
        """Export the gaps dict as a json file"""
        json_string = json.dumps(self.gaps_dict, ensure_ascii=False, indent=4)
        with open(export_path, "w", encoding='utf-8') as f:
            f.write(json_string)

    def parse_to_pairs(self):
        """This function parses the data into dataframe of bidirectional pairs (so all data is repeated) - this allows for easier filtering"""
        out_data = []
        for row in self.gaps_dict:
            gaps_data = row["gaps_data"]
            for data in gaps_data:
                for data_pair in gaps_data:
                    if not data["book"] == data_pair["book"]:
                        data_row = {"book1": data["book"], 
                                    "book2": data_pair["book"], 
                                    "start_ms1": data["start"]["ms"],
                                    "start_ms2": data_pair["start"]["ms"],
                                    "start1": data["start"]["ch"],
                                    "start2": data_pair["start"]["ch"],
                                    "end_ms1": data["end"]["ms"],
                                    "end_ms2": data_pair["end"]["ms"],
                                    "end1": data["end"]["ch"],
                                    "end2": data_pair["end"]["ch"],
                                    "text1": data["text"],
                                    "text2": data_pair["text"]}
                        if self.surround_text:
                            data_row["text_before1"] = data["text_before"]
                            data_row["text_before2"] = data_pair["text_before"]
                            data_row["text_after1"] = data["text_after"]
                            data_row["text_after2"] = data_pair["text_after"]
                        out_data.append(data_row)
        return pd.DataFrame(out_data)




    def export_csv(self, directory, pairwise=False, primary_book=None):
        all_pairs_df = self.parse_to_pairs()
        
        if pairwise:
            # Get all the books - create dirs for each book if doing the whole set (if primary, then just get pairs for that primary )
        else:
            all_pairs_df.to_csv(directory, encoding='utf-8-sig')