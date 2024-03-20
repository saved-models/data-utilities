sentinel_cages_raw = read.csv ("sentinel_cages/Sentinel_cage_sampling_info_update_01122022.csv")
#new_col_names = colnames (sentinel_cages_raw) |> 
#                lapply (\(c) stringr::str_replace_all (c, "\\.", "_"))

sentinel_cages_cleaned = sentinel_cages_raw |> 
    dplyr::mutate (Sampling.Note = stringr::str_replace (Sampling.Note, ',', ';')) #|>
    #purrr::set_names (new_col_names)

sentinel_cages_purged = sentinel_cages_cleaned |>
    dplyr::filter_all (\(x) !is.na (x))

sentinel_cages_cleaned |> 
    write.table ('sentinel_cages/sentinel_cages_cleaned.csv'
               , quote=F, row.names=F, sep=',', na='')

sentinel_cages_purged |> 
    write.table ('sentinel_cages/sentinel_cages_purged.csv'
                 , quote=F, row.names=F, sep=',', na='')
