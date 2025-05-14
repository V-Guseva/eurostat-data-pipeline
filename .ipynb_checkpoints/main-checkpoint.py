import eurostat

available_data_sets = eurostat.get_toc_df(agency='EUROSTAT')
print(type(available_data_sets))
