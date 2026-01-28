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
            if gaps_data.split(".")[-1] == "json":
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
                    if "text_after" in gap.keys() and "text_before" in gap.keys():
                        self.surround_text = True
                    else:
                        self.surround_text = False
            else:
                print("Error found in 'gaps_data' key")
                exit()
            if "books" not in row.keys():
                print("Error found in books key")
                exit()



    def write_json(self, data, export_path, indent=4):
        json_string = json.dumps(data, ensure_ascii=False, indent=indent)
        with open(export_path, "w", encoding='utf-8') as f:
            f.write(json_string)        

    def load_json(self, json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    def save_json(self, export_path):
        """Export the gaps dict as a json file"""
        self.write_json(self.gaps_dict, export_path)

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

    def _build_pairwise_dfs(self, df, primary_book):
        """Look through a df of bidirectional pairs and create separate dfs for each pair
        df: a df produced by parse_to_pairs
        primary_book: the book on the left side of the relationship
        returns: dict with structure {"URI": df, "URI": df}"""
        
        # Get just all the rows with the primary book in book1 position
        df = df[df["book1"] == primary_book]

        # Fetch the book2s
        book_pairs = df["book2"].drop_duplicates().tolist()

        # Loop through book2s, split out the dfs and add to out dict
        out_dfs = {}
        for book_pair in book_pairs:
            filtered_df = df[df["book2"] == book_pair]
            out_dfs[book_pair] = filtered_df

        return out_dfs

    
    def _build_df_all_pairs(self, df, books=None):
        """Given a df of bidirectional pairs, create pairwise files that represent every possible pairwise
        Will produce repeats - so relationship can be interogated in either direction
        df: a df produced by parse_to_pairs
        returns: dict of dicts {'URI_b1': {URI_b2: df, URI_b2: df}}"""

        # Get all the book1s
        if books is None:
            books = df["book1"].drop_duplicates().tolist()
        elif type(books) == str:
            books = [books]
        

        # Loop through book1s treating them as the primary_book - store output as dict
        all_pairs = {}
        for book in books:
            out_dfs = self._build_pairwise_dfs(df, book)
            all_pairs[book] = out_dfs
        
        return all_pairs

    def _check_create_dir(self, dir):
        if not os.path.exists(dir):
            os.mkdir(dir)

    def _write_pairwise_dirs(self, parent_dir, dfs, format):
        """Take a dict of dfs and use it to build pairwise directory structure and write out csvs
        parent_dir: the directory where all data outputs will be stored
        dfs: df structure produced by either _build_df_all_pairs or _build_pairwise_dfs
        format: 'csv' or 'label_studio' - allows same process to be applied for either label_studio jsons
        or csvs"""
        
        if format == 'label_studio':
            ext = 'json'
        # Take care when refactoring - in case we get other none extension type formats
        else:
            ext = format

        primary_books = dfs.keys()
        for book in primary_books:
            book_pairs = dfs[book].keys()
            for book_pair in book_pairs:
                file_name = f"{book}_{book_pair}.{ext}"
                base_path = os.path.join(parent_dir, book)
                self._check_create_dir(base_path)
                full_path = os.path.join(base_path, file_name)

                df = dfs[book][book_pair]
                
                if format == 'csv':
                    df.to_csv(full_path, encoding='utf-8-sig', index=False)
                if format == 'label_studio':
                    self._to_label_studio_json(df, full_path)

    def export_csv(self, directory, sep_pairwise=False, primary_books=None):
        """Convert the dataset into a pairwise representation and export it as pairwise structure.
        sep_pairwise: True/False - if true, each pair of books will be exported as a separate csv if False
                        one csv will be exported for all pairs (bi-directional)
        primary_book: only export relationships with one primary (produces one folder with csvs for each pair with the primary
                    book)"""
        
        # Run the pairwise exporter with csv format
        self._pairwise_exporter(directory, 'csv', sep_pairwise, primary_books)
    
    def _convert_to_prediction(self, text_key, before_key, after_key, ref, data_dict, label="Paraphrase"):
        """Take a row of pairwise data and use it to produce a prediction type format for label studio
        text_key: key in dictionary that contains text
        before_key: key in dictionary that contains text before the prediction
        after_key: key in dictionary that contains text after the prediction
        ref: the reference for the side of the relationship (e.g. a or b)
        data_dict: a data dictionary - from the row of the dataframe
        label: the label to give to the prediction - current defaulting to paraphrase (pipeline would need 
                updating to work with other types of prediction)
        returns: text_a, predictions dictionary
        Note: this function is written around the gaps pipeline that assumes that before, text, after are all consecutive """

        # Fetch the strings
        prediction_text = data_dict[text_key]
        before_text = data_dict[before_key]
        after_text = data_dict[after_key]

        offset_start = len(before_text)
        
        # Plus 1 to account for space we add when we join the text back together
        offset_end = offset_start + len(prediction_text) + 1
        
        full_text = " ".join([before_text, prediction_text, after_text])

        prediction = {
            "id" : f"{ref}1",
            "from_name": f"spans{ref}",
            "to_name": f"text{ref}",
            "type": "labels",
            "value": {
                "start": offset_start,
                "end": offset_end,
                "text": prediction_text,
                "labels": [label]
            }
        }

        return full_text, prediction


    def _to_label_studio(self, df):
        """Take a df, loop through each row and format it as a label_studio compliant dict"""

        # Initialise empty list to append data to
        out = []

        # Convert to list of dicts for easier processing
        rows = df.to_dict("records")

        # Loop through rows
        for row in rows:

            # If we have a before and after then some processing is required to create predictions
            if self.surround_text:
                full_text_a, prediction_a = self._convert_to_prediction("text1", "text_before1", "text_after1", "1", row)
                full_text_b, prediction_b = self._convert_to_prediction("text2", "text_before2", "text_after2", "2", row)
                row["text1"] = full_text_a
                row["text2"] = full_text_b

                processed_data = {"data": row,
                    "predictions": [
                        {"model_version": "passim_gaps",
                         "result": [
                            prediction_a,
                            prediction_b
                            ]

                        }
                    ]
                }

            
            else:
                processed_data = {"data": row}
            
            out.append(processed_data)
        
        return out


    def _to_label_studio_json(self, df, path):
        """Take df and write it to a json in label_studio format
        path: path to export json to
        df: dataframe to export"""
        label_studio_data = self._to_label_studio(df)
        self.write_json(label_studio_data, path)


    def export_label_studio_json(self, directory, sep_pairwise=False, primary_books=None):
        """Convert the dataset into a pairwise representaton and export it as a json that will import into label
        studio. If self.surround_text is True, then the text of the gap will be given as a 'prediction' and the
        full text: text_before + text + text_after will be given as the main text, with offsets for the prediction. Otherwise
        only the text will be exported as data, with no prediction
        directory: directory to export the json data to - it will be exported as a pairwise structure
                    book1/book1_book2.json if sep_pairwise is true
        sep_pairwise: if set to true separate the data into separate jsons for each book pair, otherwise 
                    export as one json all_pairs.json 
        primary_books: if given, only these books as book1 plus their book2s will be outputted"""

        # Run the exporter with label studio format
        self._pairwise_exporter(directory, 'label_studio', sep_pairwise, primary_books)
    
    def _pairwise_exporter(self, directory, format, sep_pairwise=False, primary_books=None):
        """Reusable pairwise exporter for handling different file types
        format: 'csv' or 'label_studio' 
        Allows for more flexible reuse, but custom methods"""

                # Check supplied dir exists - if not, create it
        self._check_create_dir(directory)
        
        all_pairs_df = self.parse_to_pairs()
        
        
        if sep_pairwise:
            # Filter dfs and write out the files by looping through resulting dictionaries
            dfs = self._build_df_all_pairs(all_pairs_df, primary_books)
            
            self._write_pairwise_dirs(directory, dfs, format)
            
        else:
            if format == 'csv':
                file_path = os.path.join(directory, "all_pairs.csv")
                all_pairs_df.to_csv(file_path, encoding='utf-8-sig', index=False)
            
            if format == 'label_studio':
                
                file_path = os.path.join(directory, "all_pairs_label_studio.json")
                self.to_label_studio_json(all_pairs_df, file_path)
