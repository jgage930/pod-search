from pydantic import BaseModel, ValidationError, model_validator
import feedparser
from prefect import flow, get_run_logger, task
from datetime import datetime
import json
from typing import Any, Optional

from database import DbTable, db_connect

"""
1.  Get feed.
2.  Store feed in db.
3.  Parse entries from feed.

"""


class Feed(BaseModel):
    name: str
    url: str


class FeedEntry(BaseModel):
    title: str
    summary: str
    link: str
    author: Optional[str] = ''


@task
def parse_feed(url: str) -> list[FeedEntry]:
    logger = get_run_logger()

    feed = feedparser.parse(url)
    entry_data = feed['entries']

    entries = []
    for entry in entry_data:
        try:
            entry = FeedEntry(**entry)
            entries.append(entry)
        except ValidationError as e:
            logger.error(f'Failed to validate entry {entry['title']} due to:\n{e}')

    return entries


class FeedTable(DbTable):
    __name__ = 'feeds'

    name: str
    url: str


class FeedEntryTable(DbTable):
    __name__ = 'feeds'

    title: str
    summary: str
    link: str
    author: Optional[str] = ''
    feed_id: int


@task(log_prints=True)
def setup_db():
    logger = get_run_logger()
    conn = db_connect()

    FeedTable.create_table(conn)
    FeedEntryTable.create_table(conn)


@flow(log_prints=True)
def rss_feed_pipeline(url: str, name: str):
    setup_db()

    test_feed = Feed(
        name=name,
        url=url
    )

    entries = parse_feed(test_feed.url)
    print(f'Read {len(entries)} entries from rss feed.')



if __name__ == '__main__':
    # rss_feed_pipeline.serve(
    #     name="deploy-rss-pipeline",
    #     tags=["rss", "news"],
    #     interval=3
    # )
    # rss_feed_pipeline.serve(
    #     name="Lore Rss Feed",
    #     tags=["rss", "podcasts"],
    #     interval=3,
    #     parameters={'name': 'Lore Rss Feed', 'url': "https://feeds.libsyn.com/65267/rss"},
    # )

    rss_feed_pipeline("https://feeds.libsyn.com/65267/rss", 'Lore Rss Feed')
