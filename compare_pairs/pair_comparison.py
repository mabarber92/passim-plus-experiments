
from py_kitab_diff import kitab_diff

class pairComparison():
    """A class that takes a pair of texts by default and applies a series of functions for
    comparison through methods. Methods and variables can return statistics about similarities
    and overlaps including offsets for mapping"""
    def __init__ (self, text_a, text_b):
        self.text_a = text_a
        self.text_b = text_b

        self.diff_a = None
        self.diff_b = None

    def run_diff(self):
        """Run the diff and store the raw data"""
        text1, text2, self.diff_a, self.diff_b = kitab_diff(self.text_a, self.text_b)
    
    def _filter_offsets(self, diff_data, type="="):
        """Run through a set of diff_data and the full offset data based on type
        types: = verbatim, - deletion, + addition"""
        return [offset for offset in diff_data if offset == type]

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

        return {"text_a": verbatim_a, "text_b": verbatim_b}
        






# Add a second class for handling full data dicts and sorting on the basis of diff scores
