"""Classes used for storing, processing and converting data types used across pipelines
for easy conversion to csv or LabelStudio compliant data"""
import json

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
    def __init__(self, gaps_dict):
        """Load from either json or take a gaps_dict directly. Check that the data conforms to format - if so assign it"""
    
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