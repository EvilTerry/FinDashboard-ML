import re

def normalize_merchant(text: str) -> str:
    """
    Normalizes a merchant name string by applying:
    1. Lowercasing
    2. Direct alias mapping (exact matches)
    3. Fuzzy alias mapping (regex substitutions)
    4. Noise removal (city names, legal suffixes, random digits)
    5. Whitespace cleanup
    """
    
    if not isinstance(text, str):
        return ""
    
    # Lowercase it
    text = text.lower().strip()

    # Direct Mapping
    direct_mapping = {
        "1444 deventer": "action",
        "bk 20748 apeldoorn": "burger king",
        "bk 15019 907j2g": "burger king",
        "bk 15928 sot": "burger king",
        "caelum supermarkt erp": "albert heijn",
        "stripe technology europe ltd": "amazon",
        r"x-620 maxima\kestucio g. 20-3\telsiai\\ltultu": "maxima"
    }

    if text in direct_mapping:
        text = direct_mapping[text]

    # Fuzzy Mapping
    fuzzy_mapping = {
        r'ahtogo|ah to go': "albert heijn"
    }

    for pattern, replacement in fuzzy_mapping.items():
        text = re.sub(pattern, replacement, text)

    # Noise removal
    noise_patterns = [
        r'\s*(dev183|x-088)\s*',
        r'\s+(b\.v\.|b\.v|bv|nv|inc|llc|bck|uab|lv|gt|uab|ltd)\b',
        r'(uab)+\s',
        r'\s+(nl|lt)\s*',
        # City names and locations
        r'\s*(deventer|vosselman|telsiai|klaipeda|paris|plunges|vilnius|putten|warszawa|kaunas|doornh|lithuani|kauno|deurne|wilmink|putten|drielanden|boekel|apeldoorn|devent)\s*',
        # Remove non-alphanumeric characters (punctuation)
        r'[^\w\s]+',
        # Remove sequences of 3 or more digits (transaction codes)
        r'\s+\d{3,}\s*',
    ]

    for pattern in noise_patterns:
        text = re.sub(pattern, ' ', text)

    # Final cleanup
    text = re.sub(r'\s+', ' ', text).strip()

    return text