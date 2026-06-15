import re
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

_POLICY_KEYWORDS = [
    "tariff", "tariffs", "sanction", "sanctions", "executive order",
    "trade deal", "trade war", "trade agreement", "import duty", "export ban",
    "chip ban", "semiconductor restriction", "china ban", "decoupling",
    "defense budget", "defense spending", "military spending", "pentagon budget",
    "nato spending", "interest rate", "federal reserve", "rate hike", "rate cut",
    "debt ceiling", "government shutdown", "infrastructure bill", "stimulus",
    "tax cut", "tax hike", "capital gains", "corporate tax",
]

# Accounts whose macro content should be classified as policy rather than macro_market
# so the relationship registry tests sector ETFs instead of SPY/QQQ alone
_POLICY_ACCOUNT_HANDLES = {
    "realDonaldTrump", "POTUS", "ScottBessent", "stevenmnuchin1",
}

_CONGRESSIONAL_MARKERS = [
    "rep.", "sen.", "senator", "representative", "congress",
    "disclosure", "stock act", "trade alert", "purchased shares",
    # Known names that appear in disclosure aggregator posts
    "pelosi", "tuberville", "crenshaw", "greene", "ocasio-cortez",
]

# Matches "$50K - $100K", "$1M-$5M", "$1,001 - $15,000" etc.
_AMOUNT_RE  = re.compile(r'\$[\d,]+[KMB]?\s*[-–to]+\s*\$[\d,]+[KMB]?', re.IGNORECASE)
# Matches cashtag like $TSLA, $NVDA (1-5 uppercase letters)
_CASHTAG_RE = re.compile(r'\$([A-Z]{1,5})\b')
# Matches "of TSLA" or "of $TSLA"
_OF_TICKER_RE = re.compile(r'\bof\s+\$?([A-Z]{1,5})\b')
# Non-ticker uppercase words to filter out
_NON_TICKERS = {
    "REP", "SEN", "THE", "AND", "FOR", "LLC", "INC", "ETF", "USA",
    "USD", "NYSE", "SEC", "ACT", "NEW", "OLD", "BUY", "SELL", "SOLD",
    "STOCK", "SHARES", "NET", "TAX", "TRADE", "ALERT", "PURCHASED",
    "BOUGHT", "DIVEST", "WORTH", "CEO", "CFO", "DOJ", "FBI", "SHORT",
}

# Short-seller research accounts. A report from one of these is a strong DOWN
# signal on the named ticker regardless of the post's sentiment tone.
_SHORT_SELLER_HANDLES = {
    "HindenburgRes", "muddywaters", "CitronResearch", "GothamResearch",
    "PrestigeEconom1", "FuzzyPandaShort", "ScorpionCap", "WolfpackReports",
    "BonitasResearch", "ViceroyResearch", "SprucePointCap",
}

_SHORT_REPORT_MARKERS = [
    "short", "we are short", "strong sell", "new report", "investigation",
    "fraud", "accounting", "overvalued", "scheme", "ponzi", "misleading",
    "scam", "red flag", "downside", "going to zero", "put options",
]

# "(NASDAQ: XYZ)", "(NYSE: ABC)", "(NYSE:ABC)" etc.
_EXCHANGE_TICKER_RE = re.compile(
    r'\((?:NASDAQ|NYSE|NYSEARCA|AMEX|OTC)[:\s]+([A-Za-z]{1,5})\)', re.IGNORECASE
)


def parse_short_seller_report(text: str) -> dict | None:
    """
    Parse a short-seller research post (Hindenburg, Muddy Waters, etc.).
    A report is always a DOWN signal on the named ticker.

    Returns {'ticker': str, 'direction': 'Down'} or None.
    """
    if not any(m in text.lower() for m in _SHORT_REPORT_MARKERS):
        return None

    m = _CASHTAG_RE.search(text)
    if m and m.group(1) not in _NON_TICKERS:
        return {"ticker": m.group(1), "direction": "Down"}

    m = _EXCHANGE_TICKER_RE.search(text)
    if m:
        ticker = m.group(1).upper()
        if ticker not in _NON_TICKERS:
            return {"ticker": ticker, "direction": "Down"}

    return None


def parse_congressional_trade(text: str) -> dict | None:
    """
    Parse a congressional trade disclosure post from unusual_whales
    or capitoltrades.

    Returns {'ticker': str, 'direction': 'Up'|'Down'} or None.
    """
    t = text.lower()

    # Must look like a congressional disclosure
    has_marker = any(m in t for m in _CONGRESSIONAL_MARKERS)
    has_amount  = bool(_AMOUNT_RE.search(text))
    if not has_marker and not has_amount:
        return None

    # Direction
    is_buy  = any(w in t for w in ["bought", "purchased", "acquired", "purchase"])
    is_sell = any(w in t for w in ["sold", "sale", "divested", "divest", "sell"])
    if not (is_buy or is_sell):
        return None
    direction = "Up" if is_buy else "Down"

    # Ticker — prefer cashtag ($TSLA), then "of TICKER", then nearest caps word
    m = _CASHTAG_RE.search(text)
    if m:
        ticker = m.group(1)
        if ticker not in _NON_TICKERS:
            return {"ticker": ticker, "direction": direction}

    m = _OF_TICKER_RE.search(text)
    if m:
        ticker = m.group(1)
        if ticker not in _NON_TICKERS:
            return {"ticker": ticker, "direction": direction}

    # Last resort: caps word within 60 chars of dollar amount
    amount_m = _AMOUNT_RE.search(text)
    if amount_m:
        start   = max(0, amount_m.start() - 60)
        end     = min(len(text), amount_m.end() + 60)
        context = text[start:end]
        candidates = [c for c in re.findall(r'\b([A-Z]{2,5})\b', context)
                      if c not in _NON_TICKERS]
        if candidates:
            return {"ticker": candidates[-1], "direction": direction}

    return None


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
    Classify a tweet into one of nine topic buckets.

    Priority order (first match wins):
        congressional_trade — disclosure post from unusual_whales / capitoltrades
        short_report        — short-seller research post (DOWN signal)
        company_ops         — about the CEO's own company / products
        crypto              — cryptocurrency content
        ai_tech             — AI / ML / compute content
        competitor          — mentions a named competitor
        policy              — tariffs, sanctions, executive orders (policy accounts)
        macro_market        — macroeconomic / regulatory content
        personal            — everything else (no expected market signal)

    Returns one of: 'congressional_trade', 'short_report', 'company_ops',
                    'crypto', 'ai_tech', 'competitor', 'policy',
                    'macro_market', 'personal'
    """
    if not text or len(text.strip()) < 15:
        return "personal"

    t = text.lower()

    # Gate: URL-only or very short content can't be classified
    stripped = t.replace("https://", "").replace("http://", "").strip()
    if len(stripped) < 10:
        return "personal"

    # 1 — Congressional trade disclosure (highest priority — bypasses ML)
    has_marker = any(m in t for m in _CONGRESSIONAL_MARKERS)
    has_amount  = bool(_AMOUNT_RE.search(text))
    if has_marker and has_amount:
        is_trade = any(w in t for w in ["bought", "purchased", "acquired", "sold", "divest"])
        if is_trade:
            return "congressional_trade"

    # 2 — Short-seller report (DOWN signal, bypasses ML). Only fires for known
    # short-seller accounts that name a ticker in a report-like post.
    if ceo_handle in _SHORT_SELLER_HANDLES:
        if any(m in t for m in _SHORT_REPORT_MARKERS) and (
            _CASHTAG_RE.search(text) or _EXCHANGE_TICKER_RE.search(text)
        ):
            return "short_report"

    # 3 — Company-specific
    if ceo_handle and ceo_handle in _CEO_COMPANY_KEYWORDS:
        if any(kw in t for kw in _CEO_COMPANY_KEYWORDS[ceo_handle]):
            return "company_ops"

    # 4 — Crypto (before AI so "bitcoin mining" → crypto not ai_tech)
    if any(kw in t for kw in _CRYPTO_KEYWORDS):
        return "crypto"

    # 5 — AI/tech
    if any(kw in t for kw in _AI_KEYWORDS):
        return "ai_tech"

    # 6 — Named competitor
    if ceo_handle and ceo_handle in _COMPETITOR_KEYWORDS:
        if any(kw in t for kw in _COMPETITOR_KEYWORDS[ceo_handle]):
            return "competitor"

    # 7 — Policy (tariffs, sanctions, executive orders) — for presidential/treasury accounts
    if ceo_handle and ceo_handle in _POLICY_ACCOUNT_HANDLES:
        if any(kw in t for kw in _POLICY_KEYWORDS):
            return "policy"

    # 8 — Macro/market
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
    # Congressional trade aggregators — broad ETF universe; ticker parsed directly from post
    "unusual_whales": {
        "congressional_trade": ["SPY", "QQQ", "XLF", "XLE", "XLK", "XLI", "ITA", "SOXX", "TLT", "GLD"],
        "macro_market":        ["SPY", "QQQ", "TLT", "GLD", "XLF"],
    },
    "capitoltrades": {
        "congressional_trade": ["SPY", "QQQ", "XLF", "XLE", "XLK", "XLI", "ITA", "SOXX", "TLT", "GLD"],
    },
    # Presidential / Treasury — policy tweets map to sector ETFs by theme
    "realDonaldTrump": {
        "policy":       ["XLI", "SOXX", "XLB", "SLX", "XLE", "ITA", "TLT", "GLD", "SPY", "EEM"],
        "crypto":       ["COIN", "MSTR", "MARA", "RIOT"],
        "macro_market": ["SPY", "TLT", "GLD", "XLF", "DXY"],
    },
    "POTUS": {
        "policy":       ["XLI", "SOXX", "XLB", "SLX", "XLE", "ITA", "TLT", "GLD", "SPY", "EEM"],
        "macro_market": ["SPY", "TLT", "GLD", "XLF"],
    },
    "ScottBessent": {
        "policy":       ["TLT", "XLF", "GLD", "DXY", "SPY", "XLI"],
        "macro_market": ["TLT", "XLF", "GLD", "SPY"],
    },
}