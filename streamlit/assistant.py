import json
from typing import Any, Dict, List

import vertexai
from vertexai.generative_models import (
    GenerativeModel,
    Tool,
    FunctionDeclaration,
    Part,
    GenerationConfig,
)

from bq_tools import get_station_availability_by_name

PROJECT_ID = "ba882-f25-class-project-team9"
LOCATION = "us-central1"

vertexai.init(project=PROJECT_ID, location=LOCATION)

get_station_availability_func = FunctionDeclaration(
    name="get_station_availability_by_name",
    description=(
        "Look up historical Bluebikes pickup stats for a station, "
        "given a human-readable station name (e.g. 'Boylston St at Arlington'), "
        "day_of_week (1=Sun..7=Sat), and hour range."
    ),
    parameters={
        "type": "object",
        "properties": {
            "station_name": {
                "type": "string",
                "description": "Human-readable Bluebikes station name",
            },
            "day_of_week": {
                "type": "integer",
                "minimum": 1,
                "maximum": 7,
            },
            "start_hour": {
                "type": "integer",
                "minimum": 0,
                "maximum": 23,
            },
            "end_hour": {
                "type": "integer",
                "minimum": 0,
                "maximum": 23,
            },
        },
        "required": ["station_name", "day_of_week", "start_hour"],
    },
)

tools = [Tool(function_declarations=[get_station_availability_func])]

model = GenerativeModel(
    "gemini-2.5-flash",
    tools=tools,
)

SYSTEM_PROMPT = """
You are an assistant that answers questions about historical Bluebikes
availability in Boston using BigQuery statistics.

Rules:
- Use the get_station_availability_by_name tool whenever the question involves
  a specific station, day-of-week, or time-of-day.
- Interpret phrases like "Friday evening" into day_of_week (1-7) and
  hour ranges (e.g., evening ≈ 17-20).
- Never invent numbers. Only use values returned from the tool.
- Answer clearly and concisely for a city planner.
"""


def _first_candidate_parts(resp) -> List[Part]:
    if not resp.candidates:
        return []
    content = resp.candidates[0].content
    return list(content.parts or [])


def call_llm_with_tools(user_question: str) -> str:
    resp = model.generate_content(
        [SYSTEM_PROMPT, user_question],
        generation_config=GenerationConfig(
            temperature=0.1,
        ),
    )

    parts = _first_candidate_parts(resp)

    func_call_part = None
    for p in parts:
        if hasattr(p, "function_call") and p.function_call is not None:
            func_call_part = p
            break


    if func_call_part is None:
        try:
            return resp.text
        except AttributeError:
            texts = [getattr(p, "text", "") for p in parts]
            return "\n".join(t for t in texts if t)

    fc = func_call_part.function_call
    func_name = fc.name
    args: Dict[str, Any] = dict(fc.args or {})

    if func_name != "get_station_availability_by_name":
        return "Internal error: unknown tool requested."

    tool_result = get_station_availability_by_name(
        station_name=str(args.get("station_name")),
        day_of_week=int(args.get("day_of_week")),
        start_hour=int(args.get("start_hour")),
        end_hour=(
            int(args.get("end_hour"))
            if args.get("end_hour") is not None
            else None
        ),
    )

    tool_response_part = Part.from_function_response(
        name="get_station_availability_by_name",
        response=tool_result,
    )


    resp2 = model.generate_content(
        [
            SYSTEM_PROMPT,
            user_question,
            tool_response_part,
        ],
        generation_config=GenerationConfig(
            temperature=0.2,
        ),
    )

    try:
        return resp2.text
    except AttributeError:
        parts2 = _first_candidate_parts(resp2)
        texts = [getattr(p, "text", "") for p in parts2]
        return "\n".join(t for t in texts if t)


def ask_agent(question: str) -> str:
    return call_llm_with_tools(question)
