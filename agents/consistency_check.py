from agents.state import UnderwritingState


def consistency_check(state: UnderwritingState) -> UnderwritingState:
    credit_count = state.features["bureau_total_count"]
    active_count = state.features["bureau_active_count"]
    refused_count = state.features["prev_app_refused_count"]

    rule1 = credit_count == 0 and active_count > 0
    rule2 = refused_count > 3

    flag = rule1 or rule2
    return state.model_copy(update={"consistency_flag": flag})
