from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()

def get_sentiment_score(text):
    return _analyzer.polarity_scores(text)['compound']

# ── FinBERT ───────────────────────────────────────────────────────────────────
# Lazy-loaded on first use so startup stays fast.
# Score is positive_prob - negative_prob → [-1, 1], same sign convention as VADER.

_finbert_pipeline = None

def _get_finbert_pipeline():
    global _finbert_pipeline
    if _finbert_pipeline is None:
        from transformers import pipeline as hf_pipeline
        _finbert_pipeline = hf_pipeline(
            'text-classification',
            model='ProsusAI/finbert',
            top_k=None,
            truncation=True,
            max_length=512,
        )
    return _finbert_pipeline

def get_finbert_score(text):
    """Single-text FinBERT score in [-1, 1]. Returns None on failure."""
    try:
        result = _get_finbert_pipeline()(str(text)[:512])[0]
        s = {r['label']: r['score'] for r in result}
        return round(s.get('positive', 0) - s.get('negative', 0), 4)
    except Exception:
        return None

def get_finbert_scores_batch(texts):
    """Batch FinBERT scoring — much faster than calling one-by-one during ingestion."""
    try:
        pipe = _get_finbert_pipeline()
        results = pipe([str(t)[:512] for t in texts])
        out = []
        for result in results:
            s = {r['label']: r['score'] for r in result}
            out.append(round(s.get('positive', 0) - s.get('negative', 0), 4))
        return out
    except Exception:
        return [None] * len(texts)

def get_refined_sentiment(score):
    if score <= -0.6: return 'Very Negative'
    elif score <= -0.2: return 'Negative'
    elif score < 0.2: return 'Neutral'
    elif score < 0.6: return 'Positive'
    else: return 'Very Positive'

def get_tone_category(text, score):
    text_lower = text.lower()
    emo_kws = [
        "excited", "exciting", "happy", "joyful", "amazing", "awesome",
        "incredible", "fantastic", "thrilled", "proud", "love", "great",
        "angry", "angry", "sad", "disappointed", "terrible", "horrible",
        "outraged", "furious", "disgusted", "frustrated", "upset",
    ]
    info_kws = [
        "announcement", "announce", "announcing", "launch", "launching",
        "update", "issue", "statement", "report", "release", "introducing",
        "partnership", "deal", "acquisition", "earnings", "quarterly",
        "revenue", "profit", "guidance", "forecast", "milestone",
    ]

    is_emotional = any(kw in text_lower for kw in emo_kws)
    is_informational = any(kw in text_lower for kw in info_kws)

    if is_emotional and is_informational:
        return 'Emotional (Joyful/Excited)' if score > 0.2 else 'Informational (Mixed)'
    elif is_emotional:
        return 'Emotional (Joyful/Excited)' if score > 0 else 'Emotional (Angry/Frustrated)'
    elif is_informational:
        return 'Informational (Promotional/Update)'
    return 'General Commentary'

def get_tweet_type(text):
    text_lower = text.lower()
    if any(kw in text_lower for kw in ["poll", "vote", "voting"]): return 'Poll/Vote'
    if any(kw in text_lower for kw in ["thoughts", "discuss", "opinion", "what do you", "agree", "disagree"]): return 'Discussion Starter'
    if any(kw in text_lower for kw in [
        "launch", "event", "update", "announce", "partnership", "deal",
        "acquisition", "earnings", "milestone", "record", "release", "new product",
    ]): return 'Company Milestone'
    if any(kw in text_lower for kw in ["thanks", "thank you", "congrats", "congratulations", "welcome"]): return 'Acknowledgment'
    return 'Personal/General Commentary'


# ---------------------------------------------------------------------------
# Tweet topic classifier — used by relationship_analysis.py to bucket tweets
# before testing causal links to specific tickers.
# ---------------------------------------------------------------------------

# Company-specific keywords per CEO handle. Ordered from most-specific to most-generic
# so the first match wins when topics overlap (e.g. Elon tweeting about Tesla AI).
_CEO_COMPANY_KEYWORDS = {
    "elonmusk":        ["tesla", "tsla", "spacex", "starship", "starlink", "cybertruck",
                        "model s", "model 3", "model x", "model y", "fsd", "autopilot",
                        "gigafactory", "supercharger", "neuralink", "boring company",
                        "twitter", "x corp", "xai", "grok"],
    "tim_cook":        ["apple", "aapl", "iphone", "ipad", "macbook", "macos", "ios",
                        "app store", "airpods", "apple watch", "vision pro", "siri",
                        "wwdc", "apple intelligence"],
    "satyanadella":    ["microsoft", "msft", "azure", "windows", "office 365", "teams",
                        "linkedin", "github", "copilot", "openai", "bing", "xbox",
                        "activision", "nuance"],
    "sundarpichai":    ["google", "googl", "alphabet", "youtube", "android", "chrome",
                        "gemini", "bard", "pixel", "waymo", "deepmind", "workspace"],
    "LisaSu":          ["amd", "radeon", "ryzen", "epyc", "instinct", "xilinx",
                        "mi300", "mi250", "rdna", "cdna"],
    "ajassy":          ["amazon", "amzn", "aws", "prime", "alexa", "whole foods",
                        "kindle", "echo", "fulfillment"],
    "bchesky":         ["airbnb", "abnb", "host", "listing", "booking", "travel"],
    "dkhos":           ["uber", "driver", "uber eats", "delivery", "rideshare"],
    "brian_armstrong": ["coinbase", "coin", "base chain", "custody", "cbdc", "exchange"],
    "RobertIger":      ["disney", "dis", "marvel", "pixar", "espn", "hulu",
                        "disney+", "theme park", "streaming"],
    "Benioff":         ["salesforce", "crm", "slack", "tableau", "mulesoft", "einstein",
                        "dreamforce"],
    "jack":            ["square", "cash app", "block", "sq", "spiral", "tidal", "payment"],
    "tobi":            ["shopify", "shop", "merchant", "ecommerce", "e-commerce"],
    "reedhastings":    ["netflix", "nflx", "streaming", "series", "content", "film"],
    "PGelsinger":      ["intel", "intc", "foundry", "fab", "arc graphics", "sapphire rapids",
                        "meteor lake", "gaudi"],
    "george_kurtz":    ["crowdstrike", "crwd", "falcon", "endpoint", "cybersecurity",
                        "threat detection", "breach"],
    "CathieDWood":     ["ark", "arkk", "arkg", "arkf", "arkw", "arkx", "innovation",
                        "disruptive", "cathie"],
    "AnthonyNoto":     ["sofi", "student loan", "refinance", "banking app", "sofi stadium"],
    "MichaelDell":     ["dell", "emc", "vmware", "poweredge", "latitude", "alienware"],
    "eldsjal":         ["spotify", "spot", "podcast", "playlist", "artist", "streaming"],
    "RJScaringe":      ["rivian", "rivn", "r1t", "r1s", "edv", "electric van"],
    "mtbarra":         ["general motors", "gm ", "chevy", "chevrolet", "cadillac",
                        "ultium", "cruise"],
    "JimFarley98":     ["ford", "f-150", "mustang", "bronco", "lightning", "mach-e",
                        "pro power"],
    "levie":           ["box.com", "box inc", "content cloud", "enterprise content"],
    "AlexKarp":        ["palantir", "pltr", "gotham", "foundry", "apollo", "aip"],
    "jensenhuang":     ["nvidia", "nvda", "geforce", "blackwell", "hopper", "cuda",
                        "dgx", "h100", "h200", "gb200"],
}

_CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
    "blockchain", "dogecoin", "doge", "nft", "defi", "web3",
    "hodl", "mining", "token", "litecoin", "solana", "sol",
    "satoshi", "wallet", "stablecoin", "usdc", "usdt",
]

_AI_KEYWORDS = [
    "artificial intelligence", " ai ", "machine learning", "deep learning",
    "large language model", "llm", "gpt", "generative ai", "foundation model",
    "neural network", "inference", "training run", "compute cluster",
    "transformer model", "fine-tun",
]

_MACRO_KEYWORDS = [
    "federal reserve", "fed rate", "interest rate", "inflation", "recession",
    "gdp growth", "monetary policy", "tariff", "trade war", "fiscal policy",
    "sec regulation", "congress", "legislation", "tax cut", "national debt",
    "bear market", "bull market", "market crash",
]

_COMPETITOR_KEYWORDS = {
    # Maps CEO handle → list of competitor names/tickers that signal competitor topic
    "elonmusk":     ["rivian", "rivn", "lucid", "lcid", "nio", "byd", "waymo"],
    "tim_cook":     ["samsung", "google pixel", "android phone", "qualcomm chip"],
    "satyanadella": ["google cloud", "aws", "amazon web", "salesforce crm"],
    "sundarpichai": ["microsoft bing", "azure", "openai", "chatgpt", "edge browser"],
    "LisaSu":       ["nvidia rtx", "intel arc", "qualcomm snapdragon", "arm chip"],
    "PGelsinger":   ["amd epyc", "nvidia h100", "arm server", "qualcomm server"],
    "reedhastings": ["disney+", "hulu", "hbo max", "peacock", "paramount+"],
}


def get_tweet_topic(text: str, ceo_handle: str = None) -> str:
    """
    Classify a tweet into one of six topic buckets.

    Priority order (first match wins):
        company_ops  — about the CEO's own company / products
        crypto       — cryptocurrency content
        ai_tech      — AI / ML / compute content
        competitor   — mentions a named competitor
        macro_market — macroeconomic / regulatory content
        personal     — everything else (no expected market signal)

    Returns one of: 'company_ops', 'crypto', 'ai_tech',
                    'competitor', 'macro_market', 'personal'
    """
    if not text or len(text.strip()) < 15:
        return "personal"

    t = text.lower()

    # Gate: URL-only or very short content can't be classified
    stripped = t.replace("https://", "").replace("http://", "").strip()
    if len(stripped) < 10:
        return "personal"

    # 1 — Company-specific (highest priority)
    if ceo_handle and ceo_handle in _CEO_COMPANY_KEYWORDS:
        if any(kw in t for kw in _CEO_COMPANY_KEYWORDS[ceo_handle]):
            return "company_ops"

    # 2 — Crypto (before AI so "bitcoin mining" → crypto not ai_tech)
    if any(kw in t for kw in _CRYPTO_KEYWORDS):
        return "crypto"

    # 3 — AI/tech
    if any(kw in t for kw in _AI_KEYWORDS):
        return "ai_tech"

    # 4 — Named competitor
    if ceo_handle and ceo_handle in _COMPETITOR_KEYWORDS:
        if any(kw in t for kw in _COMPETITOR_KEYWORDS[ceo_handle]):
            return "competitor"

    # 5 — Macro/market
    if any(kw in t for kw in _MACRO_KEYWORDS):
        return "macro_market"

    return "personal"


# Tickers to test for each (ceo, topic) combination beyond the CEO's own stock.
# Used by relationship_analysis.py when building the cross-asset universe.
CEO_TOPIC_UNIVERSE = {
    "elonmusk": {
        "company_ops":  ["TSLA", "RIVN", "NIO", "LCID"],
        "crypto":       ["COIN", "MSTR", "RIOT", "MARA"],
        "ai_tech":      ["TSLA", "NVDA"],
        "competitor":   ["RIVN", "NIO", "LCID", "F", "GM"],
        "macro_market": ["SPY", "QQQ"],
    },
    "tim_cook": {
        "company_ops":  ["AAPL", "QCOM", "SWKS"],
        "ai_tech":      ["AAPL", "NVDA", "MSFT"],
        "macro_market": ["SPY", "QQQ"],
    },
    "satyanadella": {
        "company_ops":  ["MSFT", "CRM", "NOW"],
        "ai_tech":      ["MSFT", "NVDA", "GOOGL"],
        "macro_market": ["SPY", "QQQ"],
    },
    "sundarpichai": {
        "company_ops":  ["GOOGL", "META", "SNAP"],
        "ai_tech":      ["GOOGL", "NVDA", "MSFT"],
        "macro_market": ["SPY", "QQQ"],
    },
    "LisaSu": {
        "company_ops":  ["AMD", "NVDA", "INTC", "QCOM"],
        "ai_tech":      ["AMD", "NVDA", "SMCI"],
        "macro_market": ["SPY", "SOXX"],
    },
    "ajassy": {
        "company_ops":  ["AMZN", "MSFT", "GOOGL"],
        "ai_tech":      ["AMZN", "NVDA"],
        "macro_market": ["SPY", "QQQ"],
    },
    "brian_armstrong": {
        "company_ops":  ["COIN", "HOOD", "MSTR"],
        "crypto":       ["COIN", "MSTR", "RIOT", "MARA"],
        "macro_market": ["SPY"],
    },
    "bchesky": {
        "company_ops":  ["ABNB", "BKNG", "EXPE"],
        "macro_market": ["SPY"],
    },
    "dkhos": {
        "company_ops":  ["UBER", "LYFT", "DASH"],
        "macro_market": ["SPY"],
    },
    "RobertIger": {
        "company_ops":  ["DIS", "NFLX", "WBD", "PARA"],
        "macro_market": ["SPY"],
    },
    "Benioff": {
        "company_ops":  ["CRM", "NOW", "WDAY", "ORCL"],
        "ai_tech":      ["CRM", "NVDA"],
        "macro_market": ["SPY"],
    },
    "jack": {
        "company_ops":  ["SQ", "PYPL", "HOOD"],
        "crypto":       ["COIN", "MSTR", "RIOT"],
        "macro_market": ["SPY"],
    },
    "tobi": {
        "company_ops":  ["SHOP", "AMZN"],
        "macro_market": ["SPY"],
    },
    "reedhastings": {
        "company_ops":  ["NFLX", "DIS", "WBD", "SPOT"],
        "macro_market": ["SPY"],
    },
    "PGelsinger": {
        "company_ops":  ["INTC", "AMD", "NVDA", "QCOM"],
        "ai_tech":      ["INTC", "NVDA", "AMD"],
        "macro_market": ["SPY", "SOXX"],
    },
    "george_kurtz": {
        "company_ops":  ["CRWD", "PANW", "S", "FTNT", "ZS"],
        "macro_market": ["SPY"],
    },
    "CathieDWood": {
        "company_ops":  ["ARKK", "TSLA", "COIN", "NVDA"],
        "crypto":       ["COIN", "MSTR"],
        "ai_tech":      ["NVDA", "TSLA"],
        "macro_market": ["SPY", "QQQ"],
    },
    "AnthonyNoto": {
        "company_ops":  ["SOFI", "HOOD", "AFRM", "UPST"],
        "macro_market": ["SPY"],
    },
    "MichaelDell": {
        "company_ops":  ["DELL", "HPQ", "HPE", "NTAP"],
        "ai_tech":      ["DELL", "NVDA"],
        "macro_market": ["SPY"],
    },
    "eldsjal": {
        "company_ops":  ["SPOT", "NFLX", "AAPL", "AMZN"],
        "macro_market": ["SPY"],
    },
    "RJScaringe": {
        "company_ops":  ["RIVN", "TSLA", "LCID", "NIO", "F", "GM"],
        "macro_market": ["SPY"],
    },
    "mtbarra": {
        "company_ops":  ["GM", "F", "TSLA", "RIVN", "STLA"],
        "macro_market": ["SPY"],
    },
    "JimFarley98": {
        "company_ops":  ["F", "GM", "TSLA", "RIVN", "STLA"],
        "macro_market": ["SPY"],
    },
    "levie": {
        "company_ops":  ["BOX", "GOOGL", "MSFT", "AMZN"],
        "macro_market": ["SPY"],
    },
    "AlexKarp": {
        "company_ops":  ["PLTR", "SAIC", "BAH", "LDOS"],
        "ai_tech":      ["PLTR", "NVDA"],
        "macro_market": ["SPY"],
    },
    "jensenhuang": {
        "company_ops":  ["NVDA", "AMD", "INTC", "QCOM", "SMCI", "ARM"],
        "ai_tech":      ["NVDA", "SMCI", "AMD"],
        "macro_market": ["SPY", "SOXX"],
    },
}