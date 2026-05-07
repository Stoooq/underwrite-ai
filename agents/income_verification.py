from agents.state import UnderwritingState


def income_verification(state: UnderwritingState) -> UnderwritingState:
    income = state.features["AMT_INCOME_TOTAL"]
    credit = state.features["AMT_CREDIT"]
    ratio = credit / income

    flag = ratio > 10
    return state.model_copy(update={"income_flag": flag})
