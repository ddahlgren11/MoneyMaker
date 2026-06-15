"""
One-shot test to validate Twitter cookies.

Usage:
    python test_twitter_cookies.py <auth_token> <ct0>

Example:
    python test_twitter_cookies.py abc123... def456...

This writes twitter_cookies.json and fetches 2 tweets from @elonmusk to confirm auth works.
"""
import asyncio, json, sys, os
from twikit import Client

COOKIES_PATH = os.path.join(os.path.dirname(__file__), "twitter_cookies.json")

async def main(auth_token: str, ct0: str):
    cookies = {"auth_token": auth_token, "ct0": ct0}
    with open(COOKIES_PATH, "w") as f:
        json.dump(cookies, f)
    print(f"Wrote {COOKIES_PATH}")

    client = Client("en-US")
    client.load_cookies(COOKIES_PATH)

    print("Fetching @elonmusk profile...")
    user = await client.get_user_by_screen_name("elonmusk")
    print(f"  User: {user.name} (@{user.screen_name}), followers: {user.followers_count:,}")

    print("Fetching recent tweets...")
    tweets = await client.get_user_tweets(user.id, tweet_type="Tweets", count=5)
    for i, t in enumerate(tweets, 1):
        ts = t.created_at_datetime
        print(f"  [{i}] {str(ts)[:16]} — {(t.full_text or t.text)[:80]}")

    print("\nCookies are valid! twitter_cookies.json is ready.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python test_twitter_cookies.py <auth_token> <ct0>")
        sys.exit(1)
    asyncio.run(main(sys.argv[1], sys.argv[2]))
