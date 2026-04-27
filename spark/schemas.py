APPLICATION_SCHEMA: dict[str, str] = {
    # --- Identifier and target ---
    "SK_ID_CURR": "int",  # client ID — joins all other tables
    "TARGET": "int",  # 0 = repaid, 1 = defaulted (what we predict)
    # --- Loan request ---
    "NAME_CONTRACT_TYPE": "string",  # Cash loans or Revolving loans
    "AMT_CREDIT": "double",  # total loan amount
    "AMT_ANNUITY": "double",  # monthly installment amount
    "AMT_GOODS_PRICE": "double",  # value of goods/property the loan is for
    "AMT_INCOME_TOTAL": "double",  # declared annual income
    # --- Client profile ---
    "CODE_GENDER": "string",  # gender (M/F)
    "FLAG_OWN_CAR": "string",  # owns a car (Y/N)
    "FLAG_OWN_REALTY": "string",  # owns real estate (Y/N)
    "CNT_CHILDREN": "int",  # number of children
    "CNT_FAM_MEMBERS": "double",  # number of family members
    "NAME_FAMILY_STATUS": "string",  # marital status
    "NAME_EDUCATION_TYPE": "string",  # education level
    "NAME_INCOME_TYPE": "string",  # income source (Working, Pensioner, etc.)
    "OCCUPATION_TYPE": "string",  # occupation
    "ORGANIZATION_TYPE": "string",  # employer's industry
    "NAME_HOUSING_TYPE": "string",  # housing situation
    # --- Time-based (negative values = days before application) ---
    "DAYS_BIRTH": "int",  # age in days — convert: abs(DAYS_BIRTH) / 365
    "DAYS_EMPLOYED": "int",  # days employed; 365243 = unemployed
    "DAYS_REGISTRATION": "double",  # days since last address change
    "DAYS_ID_PUBLISH": "int",  # days since current ID was issued
    # --- External credit scores (strongest predictors) ---
    "EXT_SOURCE_1": "double",  # credit score from external institution #1
    "EXT_SOURCE_2": "double",  # credit score from external institution #2
    "EXT_SOURCE_3": "double",  # credit score from external institution #3
    # --- Social circle defaults ---
    "OBS_30_CNT_SOCIAL_CIRCLE": "double",  # people in social circle with observed obligations (30d)
    "DEF_30_CNT_SOCIAL_CIRCLE": "double",  # people in social circle who defaulted (30d)
    "OBS_60_CNT_SOCIAL_CIRCLE": "double",  # same, 60-day window
    "DEF_60_CNT_SOCIAL_CIRCLE": "double",  # same, 60-day window
    # --- Credit bureau inquiry counts (many inquiries = actively seeking credit = risk signal) ---
    "AMT_REQ_CREDIT_BUREAU_HOUR": "double",  # inquiries in last hour
    "AMT_REQ_CREDIT_BUREAU_DAY": "double",  # inquiries in last day
    "AMT_REQ_CREDIT_BUREAU_WEEK": "double",  # inquiries in last week
    "AMT_REQ_CREDIT_BUREAU_MON": "double",  # inquiries in last month
    "AMT_REQ_CREDIT_BUREAU_QRT": "double",  # inquiries in last quarter
    "AMT_REQ_CREDIT_BUREAU_YEAR": "double",  # inquiries in last year
}

BUREAU_SCHEMA: dict[str, str] = {
    # --- Identifiers ---
    "SK_ID_CURR": "int",  # client ID — joins to application_train
    "SK_ID_BUREAU": "int",  # unique ID of this bureau credit record
    # --- Credit status ---
    "CREDIT_ACTIVE": "string",  # current status: Active, Closed, Sold, Bad debt
    "CREDIT_CURRENCY": "string",  # currency of the credit
    "CREDIT_TYPE": "string",  # type: Consumer credit, Credit card, Mortgage, etc.
    # --- Time-based (negative = days before application) ---
    "DAYS_CREDIT": "int",  # when this credit was taken (days before application)
    "DAYS_CREDIT_ENDDATE": "double",  # scheduled end date of the credit
    "DAYS_ENDDATE_FACT": "double",  # actual end date (null if still active)
    "DAYS_CREDIT_UPDATE": "int",  # days since last update of this record
    # --- Overdue information (key risk signals) ---
    "CREDIT_DAY_OVERDUE": "int",  # how many days overdue at time of application
    "AMT_CREDIT_MAX_OVERDUE": "double",  # maximum overdue amount ever on this credit
    "AMT_CREDIT_SUM_OVERDUE": "double",  # current overdue amount
    "CNT_CREDIT_PROLONG": "int",  # how many times the credit was extended
    # --- Credit amounts ---
    "AMT_CREDIT_SUM": "double",  # total credit amount
    "AMT_CREDIT_SUM_DEBT": "double",  # current remaining debt
    "AMT_CREDIT_SUM_LIMIT": "double",  # current credit limit (for revolving credits)
    "AMT_ANNUITY": "double",  # annuity of the bureau credit
}
