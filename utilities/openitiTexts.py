from openiti.helper.funcs import read_text, text_cleaner
import re
import os

class openitiTextMs():
    """A class for handling an OpenITI text as a group of milestones and applying various functions to it"""
    def __init__ (self, file_path, report=False):
        """Read the text into the object using a file. Store the fulltext and store the milestone splits
        as a special type of dictionary:
        {22: "...كتابة..."}
        On initiation, also create store maximum number of milestones in the text and the zfill level (for text mapping exercises)"""
        
        # Initiate the ms_pattern to be used across the class
        self.ms_pattern = r"ms\d+"

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist")

        # Read in OpenITI text - split off header        
        self.mARkdown_text = read_text(file_path, remove_header=True)
        
        # Run the init pipeline that populates the ms_dict
        self.init_process_milestones()

        if report:
            self.report_stats()
      
    
    def report_stats(self):
        """Read out key stats if they are populated"""
        print(f"Text has a total of: {self.ms_total} milestones")
        print(f"Text milestones zfilled to: {self.zfill_len} characters")

    def is_ms_marker(self, text):
        """Use the specified ms marker to identify if the text that is passed to the function is a ms marker"""
        
        if len(re.findall(self.ms_pattern, text)) == 1:
            return True
        else:
            return False
    
    def fetch_ms_number(self, ms_tag, return_int = True):
        """Take a string and strip the ms from it. If return_int, convert the resulting string into a integer"""
        number = re.split(r"ms", ms_tag)[-1]
        if return_int:
            number = int(number)
        return number

    def check_zfill(self, ms_splits):
        """Take a text split into milestones and splits, find the first milestone marker and use that to calculate
        the zfill (how long is the string used to represent the number)
        It also performs a check - if no ms is found through the whole text, an error is given. As we run this
        as part of the __init__ sequence it also checks the input text has valid formatting for this kind of
        processing"""
        
        # Loop until we hit a valid milestone and use that to get the zfill
        zfill = None
        for ms_split in ms_splits:
            if self.is_ms_marker(ms_split):
                number = self.fetch_ms_number(ms_split, return_int=False)
                zfill = len(number)
                break
        
        # Check that a valid ms has been found and return error if not - if found set the zfill variable
        if zfill is not None:
            self.zfill_len = zfill
        else:
            print("ERROR: Text does not contain a valid milestone splitter, first 5 items split using the ms splitter:")
            print(ms_splits[:5])
            exit()

    def build_ms_dict(self, ms_splits):
        """This is a reusable function - could be adapted to use different templates but for the moment the key is a
        milestone as an integer and the value is the text of the milestone
        Logic: if a split matches the ms marker, then the text preceding the milestone marker is the text for that milestone"""
        
        # Initiate ms_dict
        ms_dict = {}

        # Loop through the ms_splits, if the split is an ms tag, then take the previous list item as the corresponding text
        for idx, ms_split in enumerate(ms_splits):
            
            if self.is_ms_marker(ms_split) and idx > 0:
                ms_text = ms_splits[idx-1]
                ms_int = self.fetch_ms_number(ms_split)
                ms_dict[ms_int] = ms_text
        
        return ms_dict


    def init_process_milestones(self):
        """Take an OpenITI text, initiate key stats about the milestones and populate a dictionary of milestones"""
        
        # Wrap our milestone pattern in brackets so it is included within the splits
        pattern= rf"({self.ms_pattern})"
        ms_splits = re.split(pattern, self.mARkdown_text)

        # Use the splits to get the zfill (and as part of that process check for error in input)
        self.check_zfill(ms_splits)

        # Create the ms dictionary
        self.ms_dict = self.build_ms_dict(ms_splits)
        self.ms_total = len(self.ms_dict)
    
    def fetch_milestone(self, number, clean=False):
        """Use integer to fetch a milestone with that number from the dictionary. If clean, clean using the standard
        OpenITI function (same that is used for passim cleaning - so offsets match)"""
        if type(number) == str:
            number = int(number)
        text = self.ms_dict[number]
        if clean:
            text = text_cleaner(text)
        return text
    
    def fetch_offset_clean(self, ms_number, start = 0, end = -1, padding=0, trim=0):
        """Clean the ms text using the same OpenITI cleaning process used to pre-process passim inputs
        Return a character offset of the ms text between specified start and end characters. If no start is given
        start from first character of milestone, if no end is given go to the end of the milestone
        padding allows for the adding of a boundary of characters before or after the offset. The padding is
        expanded to the start or end of the nearest token to the start or end +/- padding
        trim is used for attaching context and it trims a set number of characters from the start (to the nearest token)"""
        
        # Fetch a cleaned version of the milestone text
        text = self.fetch_milestone(ms_number, clean=True)
        
        # If adding padding - find end or start of nearest token to offset - to avoid word splitting
        if padding != 0:
                        
            if end != -1:
                ms_end = len(text)
                end = end + padding
                captured_text = None
                while captured_text != " " and end < ms_end:
                    end += 1
                    captured_text = text[end]
            if start != 0:
                start = start - padding
                captured_text = None
                while captured_text != " " and start > 0:
                    start -= 1
                    captured_text = text[start]
        
        if trim != 0:
            ms_end = len(text)
            start = start + trim
            captured_text = None
            while captured_text != " " and start > 0:
                start-= 1
                captured_text = text[start]

        
        # Make offset
        text = text[start:end]

        return text
    
    def fetch_ms_list_clean(self, ms_list, start=0, end=-1, ms_joins=True, padding=0, trim=0):
        """Take a list of consecutive milestones and return a complete cleaned text according to offsets. start is the offset into the first milestone
        and end is the offset into the last milestone
        ms_joins adds the milestone marker (according to the zfill of in input text) between the milestone boundaries. If set to false then
        the texts are joined without any indication of milestone boundaries"""
        total_idx = len(ms_list) - 1
        final_list = []
        for idx, ms_number in enumerate(ms_list):

            # If it is the first item: take it with the start offset
            if idx == 0:
                text = self.fetch_offset_clean(ms_number, start=start, padding=padding)
            # else if it is the last item: take it with the end offset
            elif idx == total_idx:
                text = self.fetch_offset_clean(ms_number, end=end, padding=padding)
            # otherwise take a whole ms clean
            else:
                text = self.fetch_milestone(ms_number, clean=True)
            
            # Add the ms to the final list
            final_list.append(text)

            # If ms_joins being added, produce the new ms and add it to list
            if ms_joins and idx != total_idx:
                ms_zfill = str(ms_number).zfill(self.zfill_len) 
                ms_string = f"ms{ms_zfill}"
                final_list.append(ms_string)
        
        full_text = "".join(final_list)
        return full_text

if __name__ == "__main__":

    # Run the class on its own for testing and error checking
    openiti_text_path = "D:/OpenITI Corpus/corpus_2023_1_8/data/0310Tabari/0310Tabari.Tarikh/0310Tabari.Tarikh.Shamela0009783BK1-ara1.mARkdown"
    openiti_ms_obj = openitiTextMs(openiti_text_path, report=True)
    print("---")
    print(openiti_ms_obj.fetch_milestone(20))
    print("---")
    print(openiti_ms_obj.fetch_ms_list_clean([20,21], start = 20, end=60, ms_joins=False))
    print("---")
    print(openiti_ms_obj.fetch_ms_list_clean([20,21], start = 20, end=60))
    print("---")
    print(openiti_ms_obj.fetch_ms_list_clean([20,21,22], start = 20, end=60))

