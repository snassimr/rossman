######################################## Rossman_control.R ######################################
SYSG_SYSTEM_DIR            <- "/home/rstudio/rossmanpj"
SYSG_INPUT_DIR             <- "/home/rstudio/rossmanpj/input"
SYSG_OUTPUT_DIR            <- "/home/rstudio/rossmanpj/output"
SYSG_OUTPUT_MODELING_DIR   <- "/home/rstudio/rossmanpj/modelling"

# P   - run data preparation including read data , prepare modelling , evaluation and prediction data + new features
# DP - Data Preparation , ME - Modelling and Evaluation , P - Prediction and Submission
SYS_RUN_MODE               <- "DP" 

source("/home/rstudio/rossmanpj/Rossman_functions.R")
library(caret)
library(stringr)
library(plyr)
library(dplyr)

create_log_entry("", "Starting run ....................................","SF")

if (!SYS_RUN_MODE %in% c("DP","ME","P"))
  stop ("Illegal SYS_RUN_MODE :" , SYS_RUN_MODE)

############################################ DATA PREPARATION ############################################
# Run to prepare data and save data locally
if (SYS_RUN_MODE == "DP") {
  create_log_entry("", "Prepare data started","SF")
  perform_data_preparation()
  gc(T,T)
  create_log_entry("", "Prepare data finished","SF")
  stop ("Data preparation finished ... ")
}

############################################ MODEL ASSESSMENT ############################################

SYS_ALGORITHM_ID            <- "XGBC" 
# SYS_ALGORITHM_ID            <- "GBM"
source("/home/rstudio/rossmanpj/Rossman_functions.R")
if (SYS_RUN_MODE == "ME") {
  create_log_entry("", "Starting load data","SF")  
  
  # PREPARE MODEL ASSESSMENT
  if (exists("me_data"))  rm(me_data)
  setwd(SYSG_SYSTEM_DIR)
  me_data                <- get(load("me_input_data.rda"))
  create_log_entry("","Finsihed load data","SF")
  
  closeAllConnections()
  
  library(doMC)
  registerDoMC(cores=4)

  
  # START MODEL ASSESSMENT
  create_log_entry("", "Starting model assessment","SF")
  
  ma_model_id <- paste0("MA_","#",SYS_ALGORITHM_ID,"#",format(Sys.time(), "%Y-%m-%d %H_%M_%S"))
  me_classification_model <- create_model_assessment_data(me_data,ma_model_id)
  
  gc(T,T)
  closeAllConnections()
}

############################################ PREDICTION #####################################################

ma_run_id <- "MA_#XGBC#2015-12-03 00_02_33"
if (SYS_RUN_MODE == "P") {
  create_log_entry("", "Starting prediction on data","SF")
  opt_model_id <- paste0("MODEL_","#",SYS_ALGORITHM_ID,"#",format(Sys.time(), "%Y-%m-%d %H_%M_%S"))
  if (exists("me_data"))  rm(me_data)
  setwd(SYSG_SYSTEM_DIR)
  me_data                <- get(load("me_input_data.rda"))
  if (exists("opt_parameters"))         rm(opt_parameters)
  setwd(SYSG_OUTPUT_MODELING_DIR)
  opt_parameters         <- get(load(paste0("OM_",ma_run_id,".rda")))
  
  create_p_model(opt_model_id,opt_parameters,me_data)
  
  if (exists("p_data"))                 rm(p_data)
  if (exists("p_classification_model")) rm(p_classification_model)
  gc(T,T)
  setwd(SYSG_SYSTEM_DIR)
  p_data                 <- get(load("p_input_data.rda"))
  setwd(SYSG_OUTPUT_MODELING_DIR)
  p_classification_model <- get(load(paste0(opt_model_id,".rda")))
  
  
  prediction_data        <- create_p_prediction_data(p_classification_model, p_data , me_data)
  
  setwd(SYSG_OUTPUT_DIR)
  options(scipen=10)
  write.csv(prediction_data, file = paste0("submission_", opt_model_id ,".csv"), row.names = FALSE)
  options(scipen=0)
  create_log_entry("","Finished prediction on data","SF")
  gc(T,T)
  
}

create_log_entry("","Finished run ....................................","SF")







