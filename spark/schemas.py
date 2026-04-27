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

PREVIOUS_APPLICATION_SCHEMA: dict[str, str] = {
    # --- Identifiers ---
    "SK_ID_PREV": "int",   # unique ID of this previous application
    "SK_ID_CURR": "int",   # client ID — joins to application_train
    # --- Application details ---
    "NAME_CONTRACT_TYPE": "string",    # type: Consumer loans, Cash loans, Revolving loans
    "NAME_CONTRACT_STATUS": "string",  # outcome: Approved, Refused, Canceled, Unused offer
    "CODE_REJECT_REASON": "string",    # reason for rejection if refused
    "AMT_APPLICATION": "double",       # amount client applied for
    "AMT_CREDIT": "double",            # amount actually granted (may differ from application)
    "AMT_ANNUITY": "double",           # annuity of the previous loan
    "AMT_DOWN_PAYMENT": "double",      # down payment made
    "AMT_GOODS_PRICE": "double",       # price of goods the loan was for
    # --- Time-based ---
    "DAYS_DECISION": "int",            # days before current application when decision was made
    "DAYS_FIRST_DUE": "double",        # days before current application of first installment due
    "DAYS_LAST_DUE": "double",         # days before current application of last installment due
    "DAYS_TERMINATION": "double",      # expected termination date
    # --- Loan terms ---
    "CNT_PAYMENT": "double",           # number of installments granted
    "NAME_YIELD_GROUP": "string",      # interest rate group: low, medium, high, very high
    "CHANNEL_TYPE": "string",          # sales channel used (Country-wide, Stone, etc.)
    "NAME_GOODS_CATEGORY": "string",   # category of goods financed
    "NAME_PORTFOLIO": "string",        # portfolio type (POS, Cards, Cars, Cash)
}

INSTALLMENTS_PAYMENTS_SCHEMA: dict[str, str] = {
    # --- Identifiers ---
    "SK_ID_PREV": "int",   # previous application ID — joins to previous_application
    "SK_ID_CURR": "int",   # client ID — joins to application_train
    # --- Installment details ---
    "NUM_INSTALMENT_VERSION": "double",  # version of the installment schedule
    "NUM_INSTALMENT_NUMBER": "int",   # installment number in the sequence
    # --- Payment timing (key risk signals) ---
    "DAYS_INSTALMENT": "double",    # day the installment was supposed to be paid
    "DAYS_ENTRY_PAYMENT": "double", # day the installment was actually paid (null if unpaid)
    # --- Payment amounts ---
    "AMT_INSTALMENT": "double",  # amount that was scheduled to be paid
    "AMT_PAYMENT": "double",     # amount actually paid (difference = partial payment)
}

POS_CASH_BALANCE_SCHEMA: dict[str, str] = {
    # --- Identifiers ---
    "SK_ID_PREV": "int",  # previous application ID
    "SK_ID_CURR": "int",  # client ID — joins to application_train
    # --- Monthly snapshot ---
    "MONTHS_BALANCE": "int",             # month of the snapshot relative to application (0 = current, -1 = one month ago)
    "CNT_INSTALMENT": "double",          # total number of installments in the contract
    "CNT_INSTALMENT_FUTURE": "double",   # number of installments remaining
    "NAME_CONTRACT_STATUS": "string",    # status at this month: Active, Completed, etc.
    # --- Days past due (key risk signals) ---
    "SK_DPD": "int",      # days past due during this month
    "SK_DPD_DEF": "int",  # days past due with tolerance (defined DPD)
}

CREDIT_CARD_BALANCE_SCHEMA: dict[str, str] = {
    # --- Identifiers ---
    "SK_ID_PREV": "int",  # previous application ID
    "SK_ID_CURR": "int",  # client ID — joins to application_train
    # --- Monthly snapshot ---
    "MONTHS_BALANCE": "int",              # month relative to application (0 = current)
    "NAME_CONTRACT_STATUS": "string",     # status: Active, Completed, Signed, etc.
    # --- Balances and limits ---
    "AMT_BALANCE": "double",              # balance on the card this month
    "AMT_CREDIT_LIMIT_ACTUAL": "int",  # credit limit this month
    "AMT_RECEIVABLE_PRINCIPAL": "double", # principal receivable
    "AMT_TOTAL_RECEIVABLE": "double",     # total receivable amount
    # --- Drawings (spending behaviour) ---
    "AMT_DRAWINGS_CURRENT": "double",     # total amount drawn this month
    "AMT_DRAWINGS_ATM_CURRENT": "double", # amount drawn at ATMs
    "AMT_DRAWINGS_POS_CURRENT": "double", # amount drawn at POS terminals
    "CNT_DRAWINGS_CURRENT": "int",     # number of drawings this month
    # --- Payments ---
    "AMT_PAYMENT_CURRENT": "double",      # amount paid this month
    "AMT_INST_MIN_REGULARITY": "double",  # minimum required payment
    # --- Days past due (key risk signals) ---
    "SK_DPD": "int",      # days past due this month
    "SK_DPD_DEF": "int",  # days past due with tolerance
}

BUREAU_BALANCE_SCHEMA: dict[str, str] = {
    # --- Identifier ---
    "SK_ID_BUREAU": "int",    # bureau credit ID — joins to bureau.csv
    # --- Monthly snapshot ---
    "MONTHS_BALANCE": "int",  # month relative to application (0 = current, -1 = one month ago)
    # --- Status (key risk signal) ---
    "STATUS": "string",
    # STATUS values:
    #   C = closed
    #   X = unknown
    #   0 = no DPD (paid on time)
    #   1 = 1-30 days past due
    #   2 = 31-60 days past due
    #   3 = 61-90 days past due
    #   4 = 91-120 days past due
    #   5 = 120+ days past due
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
