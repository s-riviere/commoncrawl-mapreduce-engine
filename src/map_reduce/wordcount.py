# wordcount.py

def mapper(data: str) -> list[tuple[str, int]]:
    """Prend une ligne de texte et retourne des couples (mot, 1)."""
    words = data.split()
    return [(word.lower(), 1) for word in words]

def reducer(key: str, values: list[int]) -> tuple[str, int]:
    """Prend une clé et sa liste de valeurs, retourne la somme."""
    return key, sum(values)
