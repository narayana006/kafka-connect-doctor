import json
from pathlib import Path

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def load(name: str):
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
