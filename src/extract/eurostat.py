import eurostat

def load_df_raw_catalog():
    return eurostat.get_toc_df()
