from find_shared_gaps.find_shared_gaps import run_pipeline

if __name__ == "__main__":
    cluster_path = "E:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    meta_path = "E:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    out = "find_shared_gaps/test_gaps_padded_25.json"
    openiti_base_dir = "E:/OpenITI Corpus/corpus_2023_1_8"
    book_list = ["0630IbnAthirCizzDin.Kamil"]

    run_pipeline(cluster_path, meta_path, openiti_base_dir, book_list= book_list, raw_gaps_out=out, offset_padding=25)