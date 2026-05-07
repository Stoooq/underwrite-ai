# Agentic Loan Underwriting Pipeline: PySpark + LightGBM + LangGraph

An end-to-end automated credit decision system that processes raw banking data through a PySpark ETL pipeline, trains a LightGBM model to predict Probability of Default, and orchestrates a multi-node LangGraph pipeline to produce structured loan decisions with written justifications.

## Tech Stack

* **ETL & Feature Engineering:** PySpark
* **ML Model:** LightGBM
* **Explainability:** SHAP
* **Agent Orchestration:** LangGraph
* **State Validation:** Pydantic
* **LLM Integration:** LiteLLM
* **Data Format:** Parquet
* **Environment:** Python 3.12+, uv

## System Architecture

```
Raw CSV files (8 tables, ~57M rows total)
        │
        ▼
┌──────────────────────────────────┐
│  PySpark ETL Pipeline            │
│  ingestion → aggregation →       │
│  feature engineering → Parquet   │
└──────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────┐
│  Feature Store                   │
│  one row per client              │
└──────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────┐
│  LightGBM + SHAP                 │
│  P(default) + top risk factors   │
└──────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────┐
│              LangGraph Pipeline                      │
│                                                      │
│  [Income Verification]                               │
│          ↓                                           │
│  [Consistency Check]                                 │
│          ↓                                           │
│  [Risk Assessment] - LightGBM + SHAP                 │
│          ↓                                           │
│  [Decision] → APPROVE / REJECT / MANUAL REVIEW       │
│          ↓                                           │
│  [Report Writer] - LLM (Ollama / Claude)             │
└──────────────────────────────────────────────────────┘
        │
        ▼
Structured Output: decision + written justification
```

## Key Capabilities

* **Large-Scale PySpark ETL:** Processes all 8 Home Credit tables (bureau, installments, POS cash, credit card balances and more) into a single feature store with aggregated client-level features.
* **Probability of Default Model:** LightGBM classifier trained on 300k+ applications with train/validation split and AUC-ROC evaluation.
* **Per-Application SHAP Explainability:** Every prediction is accompanied by the top positive and negative risk factors, formatted for human-readable interpretation.
* **Stateful Multi-Node Pipeline:** LangGraph orchestrates five sequential nodes with a shared Pydantic state schema - each node reads the current state and appends its result.
* **Rule-Based Risk Flags:** Dedicated nodes check debt-to-income ratio and cross-validate declared liabilities against credit bureau data before the ML model runs.
* **LLM-Generated Reports:** The final node sends the full underwriting output to an LLM (Ollama locally or Claude API) to produce a concise, analyst-ready credit decision report.
* **Model Persistence:** Trained LightGBM model is saved with joblib and reloaded on subsequent runs - ETL and training only execute when artifacts are missing.

## Dataset

[Home Credit Default Risk](https://www.kaggle.com/c/home-credit-default-risk/data) - 8 related tables joined by client ID (`SK_ID_CURR`), totalling ~57M rows across bureau records, installment payments, credit card balances, and POS cash snapshots.

## Pipeline Nodes

| Node | Type | Output |
|---|---|---|
| Income Verification | Rule-based | `income_flag` - flags if credit/income ratio exceeds 10x |
| Consistency Check | Rule-based | `consistency_flag` - flags if bureau shows active credits with no history, or more than 3 refused applications |
| Risk Assessment | ML + SHAP | `probability_of_default`, `shap_summary` |
| Decision | Rule-based | `APPROVE` if P < 0.3 and no flags; `REJECT` if P > 0.6 or any flag raised; otherwise `MANUAL REVIEW` |
| Report Writer | LLM | `report`: written justification for analyst |
