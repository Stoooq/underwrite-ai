from langgraph.graph import StateGraph
from langgraph.graph.state import CompiledStateGraph

from agents.consistency_check import consistency_check
from agents.decision import make_decision
from agents.income_verification import income_verification
from agents.report_writer import write_report
from agents.risk_assessment import risk_assessment
from agents.state import UnderwritingState


def build_graph() -> CompiledStateGraph:
    graph = StateGraph(UnderwritingState)

    graph.add_node("income_verification", income_verification)
    graph.add_node("consistency_check", consistency_check)
    graph.add_node("risk_assessment", risk_assessment)
    graph.add_node("make_decision", make_decision)
    graph.add_node("write_report", write_report)

    graph.add_edge("income_verification", "consistency_check")
    graph.add_edge("consistency_check", "risk_assessment")
    graph.add_edge("risk_assessment", "make_decision")
    graph.add_edge("make_decision", "write_report")

    graph.set_entry_point("income_verification")
    graph.set_finish_point("write_report")

    app = graph.compile()

    return app
