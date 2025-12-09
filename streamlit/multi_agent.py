# reporting/chat-poc/multi_agent.py

from typing import TypedDict, Literal

from langgraph.graph import StateGraph, END

from assistant import ask_agent          
from policy_agent import ask_policy_agent  


class AgentState(TypedDict):
    question: str               
    route: Literal["bb", "policy"] | None
    answer: str | None

def route_agent(state: AgentState) -> AgentState:
    q = state["question"].lower()

    POLICY_KEYWORDS = [
        "policy", "rule", "rules", "regulation", "allowed", "禁止", "规定",
        "helmet", "liability", "fine", "罚款", "parking", "停在哪里",
        "mbta policy", "bluebikes policy", "使用条款", "terms of use",
        "can i", "am i allowed", "是不是可以", "能不能"
    ]

    is_policy = any(k in q for k in POLICY_KEYWORDS)

    route: Literal["bb", "policy"] = "policy" if is_policy else "bb"
    return {"route": route} 


# --------- Bluebikes agent node ---------
def run_bb_agent(state: AgentState) -> AgentState:
    q = state["question"]
    answer = ask_agent(q)
    return {"answer": answer}


# --------- Policy agent node ---------
def run_policy_agent(state: AgentState) -> AgentState:
    q = state["question"]
    answer = ask_policy_agent(q)
    return {"answer": answer}

builder = StateGraph(AgentState)

builder.add_node("router", route_agent)
builder.add_node("bb_agent", run_bb_agent)
builder.add_node("policy_agent", run_policy_agent)

builder.set_entry_point("router")

def route_decision(state: AgentState) -> str:
    return state["route"] or "bb"

builder.add_conditional_edges(
    "router",
    route_decision,
    {
        "bb": "bb_agent",
        "policy": "policy_agent",
    },
)

builder.add_edge("bb_agent", END)
builder.add_edge("policy_agent", END)

app = builder.compile()
def ask_unified_agent(question: str) -> str:
    result: AgentState = app.invoke({"question": question, "route": None, "answer": None})
    return result["answer"] or ""
