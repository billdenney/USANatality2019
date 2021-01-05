library(tidyverse)
library(tabulizer)
library(janitor)
library(assertr)
library(vroom)

# Get the instructions and data (once)
instructions_file <- "data-raw/2019/instructions/UserGuide2019-508.pdf"
data_file <- "data-raw/2019/data/Nat2019us.zip"
data_file_txt <- "data-raw/2019/data/Nat2019PublicUS.c20200506.r20200915.txt"
positions_file <- "data-raw/2019/positions.RDS"
if (!file.exists(instructions_file)) {
  curl::curl_download(
    url="ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2019-508.pdf",
    destfile = instructions_file
  )
}
if (!file.exists(data_file)) {
  curl::curl_download(
    url = "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/Nat2019us.zip",
    destfile = data_file
  )
  unzip(
    zipfile=data_file,
    files=basename(data_file_txt),
    exdir=dirname(data_file_txt)
  )
}

# Read the instructions
if (file.exists(positions_file)) {
  file_positions <- readRDS(positions_file)
} else {
  extract_pages <- 9:40
  column_loc <- c(60, 120, 174, 246, 462, 792)
  #page_dim <- tabulizer::get_page_dims(file=instructions_file, pages=extract_pages)
  instructions_raw <-
    tabulizer::extract_tables(
      file=instructions_file,
      pages=extract_pages,
      columns=list(column_loc),
      guess=FALSE,
      method="stream"
    )
  instructions_df_list <-
    lapply(
      X=instructions_raw,
      FUN=function(x) {
        drop_after <- which(x[, 2] == "Data fro")
        prep <- x
        if (length(drop_after) == 1) {
          prep <- x[seq_len(drop_after - 1), , drop=FALSE]
        }
        as.data.frame(prep) %>%
          row_to_names(1) %>%
          janitor::clean_names() %>%
          mutate_all(.funs = na_if, y="") %>%
          janitor::remove_empty(which=c("rows", "cols"))
      }
    )
  
  instructions_df_raw <- dplyr::bind_rows(instructions_df_list)
  
  fun_has_overlap <- function(x, left, right) {
    ret <- rep(NA_real_, length(x))
    for (idx in seq_along(left)) {
      if (left[idx] == right[idx]) {
        ret[[idx]] <- FALSE
      } else {
        ret[[idx]] <- any(between(x[-idx], left[idx], right[idx]))
      }
    }
    ret
  }
  
  instructions <-
    instructions_df_raw %>%
    filter(grepl(x=position, pattern="^[0-9]+(?:-[0-9]+)?$")) %>%
    filter(!grepl(x=field, pattern="^FILLER")) %>%
    mutate(
      length=as.numeric(gsub(x=length, pattern="^.* ", replacement="")),
      start=as.numeric(gsub(x=position, pattern="-.*$", replacement="")),
      end=as.numeric(gsub(x=position, pattern="^.*-", replacement=""))
    ) %>%
    verify(!is.na(length)) %>%
    verify(!is.na(start)) %>%
    # Corrections to the data in the .pdf file
    mutate(
      field=
        case_when(
          # Remove the space in the name
          field %in% "F_MM_ PLAC"~"F_MM_PLAC",
          field %in% c("f_FEDUC", "f_RF_INFT")~toupper(field),
          TRUE~field
        ),
      length=
        case_when(
          field == "MRACE6"~1,
          field == "Infections Presen"~16,
          TRUE~length
        ),
      end=
        case_when(
          field == "RF_CESARN"~333,
          TRUE~end
        )
    ) %>%
    # Remove section headers
    mutate(
      overlap=fun_has_overlap(start, start, start + length - 1)
    ) %>%
    filter(overlap == 0)
  
  file_positions <-
    fwf_positions(
      start=instructions$start,
      end=instructions$start + instructions$length - 1,
      col_names=instructions$field
    )
      
  saveRDS(object=file_positions, file=positions_file)
}

USANatality2019 <-
  vroom_fwf(
    file=data_file_txt,
    col_positions=file_positions
  )

# Clean the columns of data to be more usable

integer_columns <-
  c(
    "DOB_YY", "DOB_MM", "DOB_TT", "DOB_WK",
    "BFACIL", "BFACIL3",
    "MAGER", "MAGER14", "MAGER9",
    "MBSTATE_REC", "RESTATUS",
    "MRACE31", "MRACE6", "MRACE15", "MBRACE", "MRACEIMP", "MHISPX", "MHISP_R", "MRACEHISP",
    "DMAR", "MEDUC",
    "FAGECOMB", "FAGEREC11", #"FAGEREC31", "FAGEREC6", "FAGEREC15",
    "FRACE31", "FRACE6", "FRACE15",
    "FHISPX", "FHISP_R", "FRACEHISP",
    "FEDUC",
    "PRIORLIVE", "PRIORDEAD", "PRIORTERM",
    "LBO_REC", "TBO_REC",
    "ILLB_R", "ILLB_R11", "ILOP_R", "ILOP_R11", "ILP_R", "ILP_R11",
    "PRECARE", "PRECARE5", "PREVIS", "PREVIS_REC",
    "CIG_0", "CIG_1", "CIG_2", "CIG_3", "CIG0_R", "CIG1_R", "CIG2_R", 
    "CIG3_R",
    "M_Ht_In", "BMI_R",
    "PWgt_R", "DWgt_R", "WTGAIN", "WTGAIN_REC",
    "RF_CESARN", "NO_RISKS",
    "NO_INFEC",
    "NO_LBRDLV",
    "ME_PRES", "ME_ROUT",
    "RDMETH_REC", "DMETH_REC",
    "NO_MMORB",
    "DLMP_MM", "DLMP_YY", "COMBGEST",
    "DBWT",
    "ATTEND", "PAY", "PAY_REC",
    "APGAR5", "APGAR5R", "APGAR10", "APGAR10R",
    "DPLURAL", "SETORDER_R",
    "NO_ABNORM",
    "NO_CONGEN",
    "OCNTYFIPS", "OCNTYPOP",
    "RCNTY", "RCNTY_POP", "RCITY_POP", "RECTYPE"
  )
logical_columns <-
  grep(x=instructions$field, pattern="^F_", value=TRUE)
double_columns <-
  c(
    "BMI"
  )
factor_columns <-
  c(
    "MAR_P",
    "WIC",
    "CIG_REC",
    "RF_PDIAB", "RF_GDIAB", "RF_PHYPE", "RF_GHYPE", "RF_EHYPE", "RF_PPTERM", "RF_INFTR", "RF_FEDRG", "RF_ARTEC", "RF_CESAR",
    "IP_GON", "IP_SYPH", "IP_CHLAM", "IP_HEPB", "IP_HEPC",
    "OB_ECVS", "OB_ECVF",
    "LD_INDL", "LD_AUGM", "LD_STER", "LD_ANTB", "LD_CHOR", "LD_ANES",
    "ME_TRIAL",
    "MM_MTR", "MM_PLAC", "MM_RUPT", "MM_UHYST", "MM_AICU",
    "MTRAN",
    "SEX",
    "GESTREC10", "GESTREC3", "OEGest_Comb", "OEGest_R10", "OEGest_R3",
    "BWTR12", "BWTR4",
    "AB_AVEN1", "AB_AVEN6", "AB_NICU", "AB_SURF", "AB_ANTI", "AB_SEIZ",
    "CA_ANEN", "CA_MNSB", "CA_CCHD", "CA_CDH", "CA_OMPH", "CA_GAST", "CA_LIMB", "CA_CLEFT", "CA_CLPAL", "CA_DOWN", "CA_DISOR", "CA_HYPO",
    "ITRAN", "ILIVE", "BFED",
    "OCTERR",
    "MBCNTRY", "MRCNTRY", "MRTERR"
  )
# blank (NA) or 0 = not imputed; 1 = imputed
imputation_columns <-
  c(
    "MAGE_IMPFLG",
    "MAGE_REPFLG",
    "MAR_IMP",
    "FAGERPT_FLG",
    "IMP_PLUR",
    "IMP_SEX",
    "COMPGST_IMP", "OBGEST_FLG", "LMPUSED"
  )
to_na_value <-
  list(
    DOB_TT=9999L,
    MBSTATE_REC=3L,
    MHISPX=9L, MHISP_R=9L, MRACEHISP=8L,
    DMAR=9L, MEDUC=9L,
    FAGECOMB=99L, FAGEREC11=11L, FAGEREC31=99L, FAGEREC6=9L, FAGEREC15=99L,
    FRACE31=99L, FRACE6=9L, FRACE15=99L, FHISPX=9L, FHISP_R=9L, FRACEHISP=8L, FRACEHISP=9L, FEDUC=9L,
    PRIORLIVE=99L, PRIORDEAD=99L, PRIORTERM=99L,
    LBO_REC=9L, TBO_REC=9L,
    ILLB_R=999L, ILLB_R11=99L, ILOP_R=999L, ILOP_R11=99L, ILP_R=999L, ILP_R11=99L,
    PRECARE=99L, PRECARE5=5L, PREVIS=99L, PREVIS_REC=12L,
    CIG_0=99L, CIG_1=99L, CIG_2=99L, CIG_3=99L, CIG0_R=6L, CIG1_R=6L, CIG2_R=6L, CIG3_R=6L,
    M_Ht_In=99L, BMI=99.9, BMI_R=9L,
    PWgt_R=999L, DWgt_R=999L, WTGAIN=99L, WTGAIN_REC=9L,
    RF_CESARN=99L, NO_RISKS=9L,
    NO_INFEC=9L,
    NO_LBRDLV=9L,
    ME_PRES=9L, ME_ROUT=9L,
    RDMETH_REC=9L, DMETH_REC=9L,
    NO_MMORB=9L,
    ATTEND=9L, PAY=9L, PAY_REC=9L,
    APGAR5=99L, APGAR5R=5L, APGAR10=99L, APGAR10R=5L,
    DLMP_MM=99L, DLMP_YY=9999L, COMBGEST=99L, GESTREC10=99L, GESTREC3=3L, OEGest_Comb=99L, OEGest_R10=99L, OEGest_R3=3L,
    DBWT=9999L, BWTR12=12L, BWTR4=4L,
    NO_ABNORM=9L,
    NO_CONGEN=9L,
    OCTERR="XX"
  )

# Clean all the column types

defined_columns <-
  c(
    integer_columns,
    logical_columns,
    double_columns,
    factor_columns,
    imputation_columns
  )
stopifnot(sum(duplicated(defined_columns)) == 0)
defined_columns[duplicated(defined_columns)]
stopifnot(
  length(setdiff(
    names(USANatality2019),
    defined_columns
  )) == 0
)
stopifnot(
  length(setdiff(
    defined_columns,
    names(USANatality2019)
  )) == 0
)

cast_class <- function(x, cast_fun, na_value) {
  ret <- cast_fun(x)
  if (any(is.na(x) != is.na(ret))) {
    stop("NA values created")
  }
  if (!is.null(na_value)) {
    mask_na <- ret %in% na_value
    if (any(mask_na)) {
      ret[mask_na] <- ret[1][NA]
    }
  }
  ret
}

for (current_name in integer_columns) {
  message("Processing ", current_name)
  USANatality2019[[current_name]] <-
    cast_class(
      x=USANatality2019[[current_name]],
      cast_fun=as.integer,
      na_value=to_na_value[[current_name]]
    )
}
for (current_name in logical_columns) {
  message("Processing ", current_name)
  USANatality2019[[current_name]] <-
    cast_class(
      x=USANatality2019[[current_name]],
      cast_fun=as.logical,
      na_value=to_na_value[[current_name]]
    )
}
for (current_name in double_columns) {
  message("Processing ", current_name)
  USANatality2019[[current_name]] <-
    cast_class(
      x=USANatality2019[[current_name]],
      cast_fun=as.double,
      na_value=to_na_value[[current_name]]
    )
}
for (current_name in factor_columns) {
  message("Processing ", current_name)
  USANatality2019[[current_name]] <-
    cast_class(
      x=USANatality2019[[current_name]],
      cast_fun=factor,
      na_value=to_na_value[[current_name]]
    )
}
for (current_name in imputation_columns) {
  message("Processing ", current_name)
  USANatality2019[[current_name]] <-
    cast_class(
      x=replace_na(USANatality2019[[current_name]], 0),
      cast_fun=as.logical,
      na_value=to_na_value[[current_name]]
    )
}

# From page 33 of the instructions
if (min(USANatality2019$COMBGEST, na.rm=TRUE) < 17) {
  USANatality2019$COMBGEST <- USANatality2019$COMBGEST + 16
}
# From page 34 of the instructions
if (min(USANatality2019$DBWT, na.rm=TRUE) < 227) {
  USANatality2019$DBWT <- USANatality2019$DBWT + 226
}

usethis::use_data(
  USANatality2019,
  overwrite=TRUE
)

# An example of a potentially-interesting figure
# ggplot(USANatality2019, aes(x=COMBGEST, y=DBWT)) +
#   geom_hex() +
#   scale_fill_viridis_c(trans="log10")
