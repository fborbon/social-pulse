from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:29092"
    database_url: str = "postgresql+asyncpg://pulse:pulse@localhost:5432/social_pulse"

    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "social-pulse/1.0"

    mastodon_base_url: str = "https://mastodon.social"
    mastodon_access_token: str = ""

    track_topics: str = "artificial intelligence,climate change,cryptocurrency,elections"
    rss_feeds: str = ""

    @property
    def topics(self) -> list[str]:
        return [t.strip() for t in self.track_topics.split(",") if t.strip()]

    @property
    def rss_feed_list(self) -> list[str]:
        return [f.strip() for f in self.rss_feeds.split(",") if f.strip()]

    class Config:
        env_file = ".env"


settings = Settings()
