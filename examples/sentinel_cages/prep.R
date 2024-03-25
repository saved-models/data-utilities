library (dplyr)
library (purrr)
library (stringr)

sentinel_cages_raw = read.csv ("sentinel_cages/Sentinel_cage_sampling_info_update_01122022.csv")

#new_col_names = colnames (sentinel_cages_raw) %>%
#                lapply (\(c) stringr::str_replace_all (c, "\\.", "_"))

# Original column names with dots
sentinel_cages_cleaned = sentinel_cages_raw %>%
    mutate (across (everything (), ~str_replace (., ',', ';'))) %>%
    mutate_if (is.character, list (~na_if (., ""))) #%>%
    #set_names (new_col_names)

sentinel_cages_cleaned %>%
    write.table ('sentinel_cages/sentinel_cages_cleaned.csv'
               , quote=F, row.names=F, sep=',', na='')
