
perform_data_understanding <- function()
{
  
  names(me_input_data)
  
  cnt_data <- ddply(me_input_data, c("Date_year", "Date_month") , summarise,
                    N    = length(Sales),
                    mean = mean(Sales),
                    sd   = sd(Sales),
                    se   = sd / sqrt(N))
  
}

perform_data_preparation <- function()
{
  library(readr)
  library(stringr)
  library(dplyr)
  
  #READ TRAIN DATA
  setwd(SYSG_INPUT_DIR)
  train_input                 <- read_csv("train.csv")
  test_input                  <- read_csv("test.csv")
  store_input                 <- read_csv("store.csv")
  store_states_input          <- read_csv("store_states.csv")
  state_stats_input           <- read_csv("state_stats.csv")
  state_school_holiday_input  <- read_csv("state_school_holiday.csv")
  
  SYS_TARGET_NAME            <- "Sales"
  
  ############# Processing train data input  ###############################
  
  create_log_entry("", "Prepare train data started","SF")
  
  # Filter out non-working days
  pop_filter           <- train_input$Open=='1' & train_input$Sales!=0
  train_input1         <- train_input[pop_filter,]
  train_input1$SalesF  <- train_input1$Sales
  me_input_target_data <- log(train_input1[,"Sales"] + 1)
  train_input1         <- train_input1[,!(names(train_input1) == "Sales")]
  
  train_input1$Id      <- 1:nrow(train_input1)
  
  # Value-based features on train input data
  me_vbfe_Date  <- create_vbfe_sales(train_input1)
  # Value-based features on store input data
  # Indicators of Promo2 starting at month for each calendar month
  promo2_interval_data <- store_input$PromoInterval
  promo2_interval_data <- strsplit(promo2_interval_data, ",")
  promo2_interval_data <- data.frame(store_input$Store, do.call(rbind,lapply(promo2_interval_data, 
                                                                             function (x) {as.numeric(month.abb %in% x)})))
  names(promo2_interval_data) <- c("Store",paste0(month.abb,"_in_pi"))
  # Subselect train input
  # summary(train_input1)
  sales_features_select <- c("Store","DayOfWeek","Promo", "Customers","SalesF")
  train_input2 <- data.frame(train_input1["Id"],me_vbfe_Date,train_input1[,sales_features_select])
  
  # Subselect store input
  # summary(store_input)
  store_features_select <- c("Store","StoreType","Assortment",
                             "CompetitionDistance","CompetitionOpenSinceMonth","CompetitionOpenSinceYear",
                             "Promo2","Promo2SinceWeek","Promo2SinceYear")
  store_input1     <- store_input[,store_features_select]
  store_input2     <- process_store_missing_data(store_input1)
  
  store_state_features_select <- c("Store","State","GDP")
  store_states_input1         <- left_join(store_states_input,state_stats_input,by="State")[,store_state_features_select]
  store_input3                <- left_join(store_input2,store_states_input1,by="Store")
  
  train_input3     <- left_join(train_input2,store_input3,by="Store")
  
  vbfe_Sales_Store <- create_vbfe_sales_store(train_input3,
                                              promo2_interval_data,
                                              state_school_holiday_input)
  
  train_input4     <- data.frame(train_input3, vbfe_Sales_Store)
  
  abfe_Sales_Store <- create_abfe_sales_store(train_input4)
  
  train_input5     <- left_join(train_input4,abfe_Sales_Store$Store_DayOfWeek_meansales,
                                by = c("Store","DayOfWeek"))
  train_input5     <- left_join(train_input5,abfe_Sales_Store$Store_Promo_meansales,
                               by = c("Store","Promo"))
  train_input5     <- left_join(train_input5,abfe_Sales_Store$Store_DayOfWeek_meancustomersales,
                                by = c("Store","DayOfWeek"))
  train_input5     <- left_join(train_input5,abfe_Sales_Store$Store_meancustomersales,
                                by = c("Store"))
  
  train_input6     <- data.frame(train_input5)

  # Drop Id and SalesF column and append target feature
  me_input_data <- data.frame(train_input6[,setdiff(names(train_input6),c("Id","SalesF","Customers"))], 
                              Sales = me_input_target_data)

  create_log_entry("", "Prepare train data finished","SF")
  
  # summary(me_input_data)
  # str(me_input_data)
  
  ########### Processing test data input   #################################### 
  create_log_entry("", "Prepare test data started","SF")
  # summary(test_input)
  # str(test_input)
  
  # Value-based features on train input data
  p_vbfe_Date           <- create_vbfe_sales(test_input)
  p_features_select     <- c("Id","Open" , setdiff(names(me_input_data),c("Sales")))
  
  test_input1           <- data.frame(p_vbfe_Date,test_input)
  test_input2           <- left_join(test_input1,store_input3, by = "Store")
  p_vbfe_Sales_Store    <- create_vbfe_sales_store(test_input2,
                                                   promo2_interval_data,
                                                   state_school_holiday_input)
  test_input3           <- data.frame(test_input2, p_vbfe_Sales_Store)
  
  test_input4           <- left_join(test_input3,abfe_Sales_Store$Store_DayOfWeek_meansales,
                                     by = c("Store","DayOfWeek"))
  test_input4           <- left_join(test_input4,abfe_Sales_Store$Store_Promo_meansales,
                                     by = c("Store","Promo"))
  test_input4           <- left_join(test_input4,abfe_Sales_Store$Store_DayOfWeek_meancustomersales,
                                by = c("Store","DayOfWeek"))
  test_input4           <- left_join(test_input4,abfe_Sales_Store$Store_meancustomersales,
                                     by = c("Store"))
  
  p_input_data           <- data.frame(test_input4[,p_features_select])
  
  create_log_entry("", "Prepare test data finished","SF")
  
  for (f in names(me_input_data)) {
    if (class(me_input_data[[f]])=="character") {
      levels              <- unique(c(as.character(me_input_data[[f]]), as.character(p_input_data[[f]])))
      me_input_data[[f]]  <- factor(me_input_data[[f]], levels=levels)
      p_input_data[[f]]   <- factor(p_input_data[[f]],  levels=levels)
    }
  }
  
  # summary(p_input_data)
  # str(p_input_data)
  
  
  ## Cross promo_interval_data with Date
  ## Chart and fill non-existing monthes
  
  
  setwd(SYSG_SYSTEM_DIR)
  save(me_input_data, file = paste0("me_input_data.rda"))
  save(p_input_data, file = paste0("p_input_data.rda"))
  
  gc(T,T) 
  
}

create_vbfe_sales <- function(input_data)
{
  
  # Year , month and day of Date
  
  Date_month <- as.integer(format(input_data$Date, "%m"))
  Date_year  <- as.integer(format(input_data$Date, "%Y"))
  Date_day   <- as.integer(format(input_data$Date, "%d"))
  Month_week <- ceiling(as.numeric(format(input_data$Date, "%d"))/7)
  Year_week  <- as.integer(format(input_data$Date, "%W"))
  Year_day   <- as.integer(format(input_data$Date, "%j"))
  vbfe_Sales <- data.frame(Date_year=Date_year,Date_month=Date_month,Date_day=Date_day,
                           Month_week=Month_week,Year_week=Year_week,Year_day=Year_day)
  
  return(vbfe_Sales)
}

create_vbfe_sales_store <- function(input_data,promo2_interval_data,state_school_holiday)
{
  
  # Year , month and day of Date
  Date_year                  <- input_data$Date_year
  Date_month                 <- input_data$Date_month
  CompetitionOpenSinceYear   <- input_data$CompetitionOpenSinceYear
  CompetitionOpenSinceMonth  <- input_data$CompetitionOpenSinceMonth
  Promo2SinceYear            <- input_data$Promo2SinceYear
  Promo2SinceWeek            <- input_data$Promo2SinceWeek
  Date_month_store           <- input_data[,c("Store","Date_month")]
  Date_State                 <- input_data[,c("Date_year","Date_month","Date_day","State")]
  
  CompetitionSalesSeniorityMonths <- 
    ifelse(CompetitionOpenSinceYear==0 | CompetitionOpenSinceMonth==0 , -9999 , 12*(Date_year - CompetitionOpenSinceYear) + (Date_month - CompetitionOpenSinceMonth))
  Promo2SalesSeniorityMonths      <- 
    ifelse(Promo2SinceYear==0 | Promo2SinceWeek==0 , -9999 , 12*(Date_year - Promo2SinceYear) + (Date_month - ceiling(Promo2SinceWeek/4)))
  
  # To check Promo2SinceYear since promo2_interval_data not relevant before Promo2SinceYear
  Date_month_store_promo2_data <- left_join(Date_month_store,promo2_interval_data,by="Store")
  Promo2MonthFlg <- apply(Date_month_store_promo2_data , 1 , function(x) {
    ifelse(x[x["Date_month"]+2]==1,1,0) 
  })
  
  # Append state school holiday data
  
  state_school_holiday1 <- 
    data.frame(state_school_holiday,
               Date_year = as.integer(substr(state_school_holiday$SchoolHolidayDate, 7 , 10)),
               Date_month = as.integer(substr(state_school_holiday$SchoolHolidayDate, 4 , 5)),
               Date_day = as.integer(substr(state_school_holiday$SchoolHolidayDate, 1 , 2)))
  
  state_school_holiday_data  <- left_join(Date_State,state_school_holiday1, 
                                by = c("Date_year","Date_month","Date_day","State"))
  state_school_holiday_data$SchoolHolidayType <- ifelse(is.na(state_school_holiday_data$SchoolHolidayType),"AAA",state_school_holiday_data$SchoolHolidayType)
  state_school_holiday_data$SchoolHolidayDateFlg <- 
                                ifelse(is.na(state_school_holiday_data$SchoolHolidayDate),0,1)
  
  vbfe_Sales_Store  <- data.frame(CompetitionSalesSeniorityMonths = CompetitionSalesSeniorityMonths,
                                  Promo2SalesSeniorityMonths      = Promo2SalesSeniorityMonths,
                                  Promo2MonthFlg                  = Promo2MonthFlg,
                                  SchoolHolidayType               = state_school_holiday_data$SchoolHolidayType ,
                                  SchoolHolidayDateFlg            = state_school_holiday_data$SchoolHolidayDateFlg)
  return(vbfe_Sales_Store)
}

# input_data <- train_input4
create_abfe_sales_store <- function(input_data)
{
  
  # Mean of input Sales per Store and DayOfWeek
  Store_DayOfWeek_meansales_f <- c("Id","Store","DayOfWeek","SalesF")
  Store_DayOfWeek_meansales   <- ddply(input_data[Store_DayOfWeek_meansales_f], 
                                       c("Store", "DayOfWeek") , summarise,
                                       Store_DayOfWeek_meansales = mean(SalesF))
  
  # Mean of input Sales per Store and Promo
  Store_Promo_meansales_f     <- c("Id","Store","Promo","SalesF")
  Store_Promo_meansales       <- ddply(input_data[Store_Promo_meansales_f], 
                                       c("Store", "Promo") , summarise,
                                       Store_Promo_meansales = mean(SalesF))
  
  # Mean of input Sales per Store and Promo
  Store_meancustomersales_f               <- c("Id","Store","DayOfWeek","Customers","SalesF")
  Store_DayOfWeek_meancustomersales       <- ddply(input_data[Store_meancustomersales_f], 
                                             c("Store", "DayOfWeek") , summarise,
                                             Store_DayOfWeek_meancustomersales = mean(SalesF)/mean(Customers))
  Store_meancustomersales                 <- ddply(input_data[Store_meancustomersales_f], 
                                                   c("Store") , summarise,
                                                   Store_meancustomersales = mean(SalesF)/mean(Customers))
  
  abfe_Sales_Store <- list(Store_DayOfWeek_meansales=Store_DayOfWeek_meansales,
                           Store_Promo_meansales=Store_Promo_meansales,
                           Store_DayOfWeek_meancustomersales=Store_DayOfWeek_meancustomersales,
                           Store_meancustomersales=Store_meancustomersales)
  
  return(abfe_Sales_Store)
}


create_data_exploration <- function (input_data,iteration,output_mode)
{
  
}

# Fill CompetitionDistance with 0
#

process_store_missing_data <- function (input_store_data)
{
  
  store_data <- input_store_data
  
  store_data$CompetitionDistance[is.na(store_data$CompetitionDistance)]        <- 0
  
  store_data$CompetitionOpenSinceMonth[is.na(store_data$CompetitionOpenSinceMonth)]  <- 0
  
  store_data$CompetitionOpenSinceYear[is.na(store_data$CompetitionOpenSinceYear)]   <- 0
  
  store_data$Promo2SinceWeek[is.na(store_data$Promo2SinceWeek)]            <- 0
  
  store_data$Promo2SinceYear[is.na(store_data$Promo2SinceYear)]            <- 0
  
  return (store_data)
}

create_model_assessment_data <- function (me_input_data,ma_run_id)
{
  
  library(caret)
  
  e_indexes <- which(me_input_data$Date_year== 2015 & 
                       ((me_input_data$Date_month==7) | (me_input_data$Date_month==6 & me_input_data$Date_day > 14)))
  
  m_indexes <- setdiff(1:dim(me_input_data)[1] , e_indexes)
  
  classification_formula <- as.formula(paste("Sales" ,"~",
                                             paste(names(me_input_data)[!names(me_input_data)=='Sales'],collapse="+")))
  
  # Initialize model assesment objects
  start_time           <- NULL
  end_time             <- NULL
  classification_model <- NULL
  assesment_grid       <- NULL
  start_time           <- proc.time()
  
  
  # Greed for parameter evaluation
  if (grepl("XGBC",ma_run_id)) {
    xgb_tuneGrid   <- expand.grid(  nrounds   = seq(200,400, length.out = 3) , 
                                    eta       = seq(0.02,0.05, length.out = 4) , 
                                    max_depth = seq(9,12, length.out = 4))
    xgb_tuneGrid   <- expand.grid(  nrounds   = 500 , 
                                    eta       = 0.05 , 
                                    max_depth = 10)
    assesment_grid <- xgb_tuneGrid
  }
  
  if (grepl("GBM",ma_run_id)) {
    gbm_tuneGrid   <- expand.grid(interaction.depth = seq(5,5, length.out = 1),
                                  n.trees = seq(301,301, length.out = 1),
                                  shrinkage = seq(0.05,0.05, length.out = 1) , n.minobsinnode = 5)
    assesment_grid <- gbm_tuneGrid
  }
  
  
  ma_control <- trainControl(method          = "cv",
                             number          = 1,
                             index           = list(rs1=m_indexes),
                             indexOut        = list(rs2=e_indexes),
                             summaryFunction = RMPSE,
                             allowParallel   = TRUE , 
                             verboseIter     = TRUE,
                             savePredictions = TRUE)
  
  ############################################################# MODEL CREATION #####################################
  
  create_log_entry("",paste0(ma_run_id ," Model Assesment started"),"SF")
  create_log_entry(names(assesment_grid),assesment_grid,"F")
  
  if (grepl("XGBC",ma_run_id)) { 
    xgbc_model <- train( classification_formula , data = me_input_data , method = "xgbTree", 
                         metric   ="RMPSE" , trControl = ma_control, tuneGrid = assesment_grid , 
                         maximize            = FALSE,
                         objective           = 'reg:linear',
                         min_child_weight    = 5,
                         lambda              = 0.1,
                         nthread             = 6)
    
    classification_model <- xgbc_model
  }
  if (grepl("GBM",ma_run_id)) { 
    gbm_model <- train(classification_formula , data = me_input_data , method = "gbm", 
                       metric = "RMPSE" , trControl = ma_control, tuneGrid = assesment_grid,
                       maximize = FALSE)
    classification_model <- gbm_model
  }
  
  end_time <- proc.time() ; runtime <- round(as.numeric((end_time - start_time)[3]),2)
  
  opt_parameters         <- classification_model$bestTune
  
  create_log_entry("",paste0(ma_run_id , " Model Assesment finished : " , runtime),"SF")
  
  # Output feature importance based on modelling data
  importance_data_obj <- varImp(classification_model,scale = FALSE)$importance
  importance_data     <- data.frame(Var = rownames(importance_data_obj),Imp = importance_data_obj$Overall,stringsAsFactors=FALSE)
  
  create_log_entry("",paste0(ma_run_id , " Feature Importance : "),"F")
  create_log_entry(names(importance_data),head(importance_data,200),"F")
  
  create_log_entry("",paste0(ma_run_id , " Evaluation : " , classification_model$results$RMPSE),"SF")
  
  setwd(SYSG_OUTPUT_MODELING_DIR)
  save(classification_model, file = paste0(ma_run_id,".rda"))
  save(opt_parameters, file = paste0("OM_",ma_run_id,".rda"))
  
  # Create predictions based on evaluation data
  #   create_e_prediction_data(classification_model, e_input_data , ma_run_id)
  
  
}

RMPSE <- function(data,lev = NULL, model = NULL) {
  
  epred      <- data$pred
  eobs       <- data$obs
  pred       <- exp(as.numeric(epred))-1
  obs        <- exp(as.numeric(eobs))-1
  #   print(names(data))
  #   print(head(epred,10))
  #   print(head(obs,10))
  #  print(mean(((obs-pred)/obs)^2))
  #   print(sum(obs==0))
  
  err        <- sqrt(mean(((obs-pred)/obs)^2))
  # err <- 0.5
  names(err) <- "RMPSE"
  err
}


# Create final model using optimal parameters tuned by caret + non-tunable parameters after manual evaluation
# Use all train data set
create_p_model <- function (opt_model_id , opt_parameters, me_input_data)
{
  classification_formula <- as.formula(paste("Sales" ,"~",
                                             paste(names(me_input_data)[!names(me_input_data)=='Sales'],collapse="+")))
  
  set.seed(2223)
  p_seeds <- vector(mode = "list", length = 1)
  p_seeds[[1]] <- sample.int(1000, 1)
  
  m_control <- trainControl(method          = "none",
                            seeds           = p_seeds,
                            allowParallel   = TRUE , 
                            verboseIter     = TRUE)
  
  create_log_entry("",paste0(opt_model_id ," Optimal Model Creation started : "),"SF")
  create_log_entry(names(opt_parameters), opt_parameters ,"F")
  
  start_time <- proc.time()
  
  if (grepl("XGBC",opt_model_id)) { 
    xgbc_model <- train( classification_formula , data = me_input_data , method = "xgbTree", 
                         trControl = m_control, tuneGrid = opt_parameters , 
                         objective           = 'reg:linear',
                         min_child_weight    = 5,
                         lambda              = 0.1,
                         nthread             = 6)
    
    opt_classification_model <- xgbc_model
  }
  if (grepl("GBM",ma_run_id)) { 
    gbm_model <- train(classification_formula , data = me_input_data , method = "gbm", 
                       trControl = m_control, tuneGrid = opt_parameters)
    
    opt_classification_model <- gbm_model
  }
  
  end_time <- proc.time() ; runtime <- round(as.numeric((end_time - start_time)[3]),2)
  
  save(opt_classification_model, file = paste0(opt_model_id,".rda"))
  
  create_log_entry("",paste0(opt_model_id , " Optimal Model Creation finished : " , runtime),"SF")
  
}

# Function predicts model on evaluation data and output AUC to log
create_e_prediction_data <- function (classification_model, e_input_data , ma_run_id)
{
  prediction_values <- NULL
  prediction_values  <- predict(classification_model,e_input_data , type = "raw")
  
  data <- data.frame(pred = prediction_values,obs = e_input_data$Sales)
  
  RMSPE <- RMPSE(data)
  
  create_log_entry("",paste0(ma_run_id , " Evaluation : " , RMSPE),"SF")
}

# Function predicts model on prediction/submission data
create_p_prediction_data <- function (classification_model,p_input_data,m_input_data)
{
  
  pop_filter    <- p_input_data$Open==1 | is.na(p_input_data$Open)
  
  prediction_values <- NULL
  for (i in 1:dim(p_input_data)[1]) {
    prediction_value   <- ifelse(pop_filter[i],exp(as.numeric(predict(classification_model,p_input_data[i,])))-1,0)
    prediction_values  <- rbind(prediction_values,prediction_value)
  }
  
  prediction_data           <- data.frame(Id = p_input_data$Id,Sales = prediction_values , row.names = NULL)
  
  return (prediction_data)
}

create_log_entry <- function(message_title = "", message , log_mode)
  
{
  current_library <- getwd()
  
  setwd(SYSG_SYSTEM_DIR)
  
  if (regexpr("S",log_mode)>0) {
    print(message_title , row.names = FALSE)
    print(message , row.names = FALSE)
  }
  
  if (regexpr("F",log_mode)>0) {
    write.table(message_title , "log.txt", append = TRUE,col.names = FALSE ,  row.names = FALSE , quote = FALSE)
    if (class(message)=="data.frame")
      write.table(message , "log.txt", append = TRUE,col.names = FALSE ,  row.names = FALSE , quote = FALSE , sep = "\t") 
    else write.table(paste0(Sys.time(), " : " , message) , "log.txt", append = TRUE, col.names = FALSE ,  row.names = FALSE , quote = FALSE,sep = ",")
  }
  
  setwd(current_library)
  
}  

process_p_missing_data <- function (p_input_data,p_missing_features)
{
  
  input_data <- p_input_data
  
  if (exists("m_missing_val"))              
    rm(m_missing_val)
  setwd(SYSG_OUTPUT_MODELING_DIR)
  m_missing_val           <- get(load("m_missing_val.rda"))
  
  for (i in 1:ncol(input_data)) {
    input_data[is.na(input_data[,i]),i] <- m_missing_val[[p_missing_features[i]]]
  }
  
  return (input_data)
}
