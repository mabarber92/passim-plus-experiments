# find_shared_gaps

A pipeline that takes passim cluster data and analyses it for cases where a pair of authors share a gap between reuse instances that is not filled by an alignment with another text. These are cases where we might suspect one text has paraphrased another.

Heuristic:
- There is a gap between reuse shared by the same pair of books. Book A --> gap --> Book A aligns with Book B --> gap --> Book B
- That gap is not filled with a text that was written before book A (we keep cases where the gap is filled by a text written later, as it's possible that the paraphrase has been reused verbatim by a later author)
- The gap is larger than a ```min_gap``` in characters