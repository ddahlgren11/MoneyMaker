"""
Single source of truth for CEO → ticker mappings.
Imported by main.py, run_pipeline.py, and anywhere else that needs this list.
"""

CEO_TARGETS = {
    "elonmusk":        {"name": "Elon Musk",          "ticker": "TSLA"},
    "tim_cook":        {"name": "Tim Cook",            "ticker": "AAPL"},
    "satyanadella":    {"name": "Satya Nadella",       "ticker": "MSFT"},
    "sundarpichai":    {"name": "Sundar Pichai",       "ticker": "GOOGL"},
    "MichaelDell":     {"name": "Michael Dell",        "ticker": "DELL"},
    "LisaSu":          {"name": "Lisa Su",             "ticker": "AMD"},
    "ajassy":          {"name": "Andy Jassy",          "ticker": "AMZN"},
    "bchesky":         {"name": "Brian Chesky",        "ticker": "ABNB"},
    "dkhos":           {"name": "Dara Khosrowshahi",  "ticker": "UBER"},
    "RobertIger":      {"name": "Robert Iger",         "ticker": "DIS"},
    "Benioff":         {"name": "Marc Benioff",        "ticker": "CRM"},
    "jack":            {"name": "Jack Dorsey",         "ticker": "SQ"},
    "tobi":            {"name": "Tobi Lütke",          "ticker": "SHOP"},
    "brian_armstrong": {"name": "Brian Armstrong",     "ticker": "COIN"},
    "ericyuan":        {"name": "Eric Yuan",           "ticker": "ZM"},
    "CathieDWood":     {"name": "Cathie Wood",         "ticker": "ARKK"},
    "AlexKarp":        {"name": "Alex Karp",           "ticker": "PLTR"},
    "mtbarra":         {"name": "Mary Barra",          "ticker": "GM"},
    "JimFarley98":     {"name": "Jim Farley",          "ticker": "F"},
    "AnthonyNoto":     {"name": "Anthony Noto",        "ticker": "SOFI"},
    "reedhastings":    {"name": "Reed Hastings",       "ticker": "NFLX"},
    "PGelsinger":      {"name": "Pat Gelsinger",       "ticker": "INTC"},
    "levie":           {"name": "Aaron Levie",         "ticker": "BOX"},
    "george_kurtz":    {"name": "George Kurtz",        "ticker": "CRWD"},
    "eldsjal":         {"name": "Daniel Ek",           "ticker": "SPOT"},
    "RJScaringe":      {"name": "RJ Scaringe",         "ticker": "RIVN"},
}

# Flat handle → ticker dict for pipeline loops
HANDLE_TO_TICKER = {h: v["ticker"] for h, v in CEO_TARGETS.items()}
