from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:29092"
    database_url: str = "postgresql+asyncpg://pulse:pulse@localhost:5432/social_pulse"

    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "social-pulse/1.0"

    mastodon_base_url: str = "https://mastodon.social"
    mastodon_access_token: str = ""

    # API-key sources
    guardian_api_key: str = ""
    newsapi_key: str = ""
    nytimes_api_key: str = ""

    track_topics: str = "artificial intelligence,climate change,cryptocurrency,elections"
    rss_feeds: str = (
        "https://feeds.bbci.co.uk/news/rss.xml,"
        "https://feeds.reuters.com/reuters/topNews,"
        "https://feeds.npr.org/1001/rss.xml,"
        "https://www.theverge.com/rss/index.xml,"
        "https://feeds.arstechnica.com/arstechnica/index,"
        "https://www.bls.gov/feed/bls_latest.rss"
    )

    @property
    def topics(self) -> list[str]:
        return [t.strip() for t in self.track_topics.split(",") if t.strip()]

    @property
    def rss_feed_list(self) -> list[str]:
        return [f.strip() for f in self.rss_feeds.split(",") if f.strip()]

    class Config:
        env_file = ".env"


settings = Settings()
