TOKEN_MAP = {w: p for p, w in enumerate(
"the and is to of a in that it with on for are as this was at be by an".split(), start=11)}

def hash_sentence(s: str) -> list[dict]:
return [{"prime": TOKEN_MAP.get(w.lower(), 2), "k": 1}
for w in s.split() if w.isalpha()][:30] # RAM-safe cap
