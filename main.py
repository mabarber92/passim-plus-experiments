from find_shared_gaps.find_shared_gaps import run_pipeline
from utilities.data_parsing import gapsClusters

if __name__ == "__main__":
    cluster_path = "D:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    meta_path = "D:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    out = "find_shared_gaps/test_gaps_context.json"
    openiti_base_dir = "D:/OpenITI Corpus/corpus_2023_1_8"
    book_list = ["0630IbnAthirCizzDin.Kamil"]

    # run_pipeline(cluster_path, meta_path, openiti_base_dir, book_list= book_list, raw_gaps_out=out, fetch_context=True)

    pairwise_dir = "find_shared_gaps/pairwise_label_studio_context/"
    gaps_obj = gapsClusters(out)
    print("Exporting to label_studio format...")
    gaps_obj.export_label_studio_json(pairwise_dir, sep_pairwise=True, primary_books=book_list)