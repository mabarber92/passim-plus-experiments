
from py_kitab_diff import kitab_diff

class pairComparison():
    """A class that takes a pair of texts by default and applies a series of functions for
    comparison through methods. Methods and variables can return statistics about similarities
    and overlaps including offsets for mapping"""
    def __init__ (self, text_a, text_b, min_line_length=20):
        """Setting min_line_length to None will mean that diff is calculated without peforming
        line breaks (this should only be set to an int if using the output in a reader, otherwise
        it's not likely to be useful)"""
        self.text_a = text_a
        self.text_b = text_b

        self.diff_a = None
        self.diff_b = None

        self.min_line_length = min_line_length

        # print("Text A")
        # print(text_a)
        # print("Text B")
        # print(text_b)

    def run_diff(self):
        """Run the diff and store the raw data"""
        if self.min_line_length is None:
            self.text_a, self.text_b, self.diff_a, self.diff_b = kitab_diff(self.text_a, self.text_b)
        else:
            self.text_a, self.text_b, self.diff_a, self.diff_b = kitab_diff(self.text_a, self.text_b, min_line_length=self.min_line_length, line_tag="\n")
    
    def _filter_offsets(self, diff_data, type="="):
        """Run through a set of diff_data and the full offset data based on type
        types: = verbatim, - deletion, + addition"""        
        return [offset for offset in diff_data if offset["type"] == type]

    def fetch_verbatim_offsets(self):
        """Get the start and end positions for all verbatim overlap in text_a and text_b
        Return a dict with the lists of offsets:
        {"text_a": [], "text_b": []}"""
        
        # If this is run before diff_a or diff_b exists - run the diff first (saves us having to use two methods just to get this),
        # But if getting multiple diff fetches are run in sequence then we only run the diff once
        if self.diff_a is None:
            self.run_diff()
        
        verbatim_a = self._filter_offsets(self.diff_a)
        verbatim_b = self._filter_offsets(self.diff_b)

        if "ms" in self.text_a:
            print(self.text_a)
        if "ms" in self.text_b:
            print(self.text_b)
        
        return {"text_a": self.text_a,
                "offsets_a": verbatim_a,
                "text_b": self.text_b,
                "offsets_b": verbatim_b}
        






# Add a second class for handling full data dicts and sorting on the basis of diff scores
