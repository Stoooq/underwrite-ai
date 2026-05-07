from agents.state import UnderwritingState


def make_decision(state: UnderwritingState) -> UnderwritingState:
    prob = state.probability_of_default
    income_flag = state.income_flag
    consistency_flag = state.consistency_flag

    if prob < 0.3 and not income_flag and not consistency_flag:
        decision = "APPROVE"
    elif prob > 0.6 or income_flag or consistency_flag:
        decision = "REJECT"
    else:
        decision = "MANUAL REVIEW"

    return state.model_copy(update={"decision": decision})
