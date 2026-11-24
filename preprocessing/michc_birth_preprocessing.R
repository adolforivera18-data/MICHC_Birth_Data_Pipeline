# step 1
#Packages needed for this project
install.packages("data.table") # Install the package if you haven't already
library(data.table)


library(tidyverse)

install.packages("readr")
library(readr)

install.packages("writexl")
library(writexl)

#This attempted to  import the data with a fixed width format reader when the widths were unknown- DO NOT USE
michc <- read_fwf("C:/Users/AARivera/Desktop/R STudio/Data/2025_SPDS_1.dat", 
                  col_positions = fwf_empty("C:/Users/AARivera/Desktop/R STudio/Data/2025_SPDS_1.dat"))
head(michc)


# STEP 2
#FRAMEWORK CODE- for the labels and the parsing
#This is a list of all the column widths as attained from the data document
columnwidths<- c(1,4,4,30,30,4,4,9,6,4,9,2,20,10,2,4,10,9,2,2,9,2,20,10,2,4,9,2,2,28,30,4,30,30,40,
                 4,17,8,4,1,2,2,4,30,30,40,35,17,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
                 8,1,24,24,1,30,30,40,4,1,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,30,8,1,24,24,4,
                 9,2,20,10,2,4,9,2,2,1,8,8,8,8,2,2,2,2,2,2,2,2,2,2,6,6,6,1,6,1,4,1,1,8,
                 1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,1,8,1,8,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
                 1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
                 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,3,
                 3,3,3,1,1,2,3,3,3,1,1,1,1,1,5,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,30,4,100)


#STEP 3
#Import the column lables from an EXCEL file
library(readxl)

label_vector<-read_excel(
  "C:/Users/AARivera/Desktop/MICHC PROJECT/MICHC LABELS.xlsx",
  range= "A1:A281",
  col_names=FALSE,
  col_types="text"
)

#STEP 4
#Converts the dataframe column into a vector
column_names_list<-label_vector[[1]]

#STEP 5
# Columns for new data base
new_columns <- c(3,6,8,10,18,19,30,38,40,43,51:72,75,118,
                 120:124,137,140:145,149,158,166:183,185:192,
                 201:218,225:228,237:245,250,251,253:255,257:259)


#STEP 6- Create function
# ============================================================
# MICHC DATE CONVERSION FUNCTION
# ============================================================

convert_michc_date <- function(x) {
  
  x <- trimws(as.character(x))
  
  formatted_date <- ifelse(
    nchar(x) == 8,
    x,
    ifelse(
      nchar(x) == 7,
      paste0("0", x),
      ifelse(
        nchar(x) == 6,
        paste0("0", substr(x, 1, 1),
               "0", substr(x, 2, 6)),
        NA_character_
      )
    )
  )
  
  as.Date(formatted_date, format = "%m%d%Y")
}


#STep 7- create function
# ============================================================
# MICHC MATERNAL AGE CALCULATION
# ============================================================

calculate_maternal_age <- function(mother_dob, infant_dob) {
  
  mother_date <- convert_michc_date(mother_dob)
  infant_date <- convert_michc_date(infant_dob)
  
  # Calculate difference between birth years
  age <- as.integer(format(infant_date, "%Y")) -
    as.integer(format(mother_date, "%Y"))
  
  # Subtract one if mother's birthday has not occurred yet
  birthday_not_reached <-
    format(infant_date, "%m-%d") <
    format(mother_date, "%m-%d")
  
  age <- age - as.integer(birthday_not_reached)
  
  # Exclude invalid or implausible ages
  age[!is.na(age) & (age < 10 | age > 60)] <- NA_integer_
  
  return(age)
}

calculate_maternal_age(
  mother_dob = c("12151995", "1151995", "152000"),
  infant_dob = c("11012023", "12012023", "152023")
)

#STep 8- build excel ready datasets
#2025 DATA
#This imported the data using ficed with reader when the widths are unknown- USE
michc2025<- read.fwf("C:/Users/AARivera/Desktop/MICHC PROJECT/2025_SPDS_1.dat",
                     widths= columnwidths,
                     header= FALSE,
                     fill=TRUE,
                     stringsAsFactors= FALSE)
head(michc2025)



#Assigns ths labels to the data frame
names(michc2025)<- column_names_list

michc2025_final<- michc2025[, c(new_columns)]

write_xlsx(michc2025_final, "C:/Users/AARivera/Desktop/MICHC PROJECT/michc2025_final.xlsx")

#STEP 9- add new moms age column
# ============================================================
# 2025 MATERNAL AGE AT DELIVERY
# ============================================================

michc2025_final$maternal_age <- calculate_maternal_age(
  michc2025_final[["Mother's Date of Birth"]],
  michc2025_final[["Infant's Date of Birth (MMDDCCYY)"]]
)

# Validate maternal ages
summary(michc2025_final$maternal_age)

# Count missing maternal ages
sum(is.na(michc2025_final$maternal_age))


#step 8
#2024 DATA
#This imported the data using ficed with reader when the widths are unknown- USE
michc2024<- read.fwf("C:/Users/AARivera/Desktop/MICHC PROJECT/2024_SPDS_1.dat",
                     widths= columnwidths,
                     header= FALSE,
                     fill=TRUE,
                     stringsAsFactors= FALSE)
head(michc2024)



#Assigns ths labels to the data frame
names(michc2024)<- column_names_list

michc2024_final<- michc2024[, c(new_columns)]


write_xlsx(michc2024_final, "C:/Users/AARivera/Desktop/MICHC PROJECT/michc2024_final.xlsx")

#STEP 9- add new moms age column
# ============================================================
# 2024 MATERNAL AGE AT DELIVERY
# ============================================================

michc2024_final$maternal_age <- calculate_maternal_age(
  michc2024_final[["Mother's Date of Birth"]],
  michc2024_final[["Infant's Date of Birth (MMDDCCYY)"]]
)

# Validate maternal ages
summary(michc2024_final$maternal_age)

# Count missing maternal ages
sum(is.na(michc2024_final$maternal_age))



#step 8
#2023 DATA
#This imported the data using ficed with reader when the widths are unknown- USE
michc2023<- read.fwf("C:/Users/AARivera/Desktop/MICHC PROJECT/2023_SPDS_1.dat",
                     widths= columnwidths,
                     header= FALSE,
                     fill=TRUE,
                     stringsAsFactors= FALSE)
head(michc2023)



#Assigns ths labels to the data frame
names(michc2023)<- column_names_list

michc2023_final<- michc2023[, c(new_columns)]

write_xlsx(michc2023_final, "C:/Users/AARivera/Desktop/MICHC PROJECT/michc2023_final.xlsx")

#STEP 9- add new moms age column
# ============================================================
# 2023 MATERNAL AGE AT DELIVERY
# ============================================================

michc2023_final$maternal_age <- calculate_maternal_age(
  michc2023_final[["Mother's Date of Birth"]],
  michc2023_final[["Infant's Date of Birth (MMDDCCYY)"]]
)

# Validate calculated ages without displaying birth dates
summary(michc2023_final$maternal_age)

# Count missing or invalid ages
sum(is.na(michc2023_final$maternal_age))


#step 10 create AWS ready dateframes
# ============================================================
# CREATE DE-IDENTIFIED MICHC DATAFRAMES
# ============================================================

# Columns to remove
dob_columns <- c(
  "Mother's Date of Birth",
  "Infant's Date of Birth (MMDDCCYY)"
)

# Create separate AWS-ready dataframes
michc2023_aws <- michc2023_final[
  , !names(michc2023_final) %in% dob_columns
]

michc2024_aws <- michc2024_final[
  , !names(michc2024_final) %in% dob_columns
]

michc2025_aws <- michc2025_final[
  , !names(michc2025_final) %in% dob_columns
]

# Confirm date-of-birth columns were removed
sapply(
  list(michc2023_aws, michc2024_aws, michc2025_aws),
  function(df) any(dob_columns %in% names(df))
)

# Confirm maternal_age was retained
sapply(
  list(michc2023_aws, michc2024_aws, michc2025_aws),
  function(df) "maternal_age" %in% names(df)
)

#STep 11 write CSV file to project folder
# ============================================================
# EXPORT MICHC DATASETS FOR AWS
# ============================================================

# 2023
write.csv(
  michc2023_aws,
  "C:/Users/AARivera/Desktop/MICHC DATA PIPELINE/michc2023_aws.csv",
  row.names = FALSE,
  na = ""
)

# 2024
write.csv(
  michc2024_aws,
  "C:/Users/AARivera/Desktop/MICHC DATA PIPELINE/michc2024_aws.csv",
  row.names = FALSE,
  na = ""
)

# 2025
write.csv(
  michc2025_aws,
  "C:/Users/AARivera/Desktop/MICHC DATA PIPELINE/michc2025_aws.csv",
  row.names = FALSE,
  na = ""
)
