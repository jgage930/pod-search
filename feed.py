from pydantic import BaseModel, ValidationError, model_validator
import feedparser
from prefect import flow, get_run_logger, task
from datetime import datetime
import json
from typing import Any


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
    author: str


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


@flow
def rss_feed_pipeline():
    test_feed = Feed(
        name="NPR News",
        url="https://feeds.npr.org/1001/rss.xml"
    )

    entries = parse_feed(test_feed.url)
    print(entries)


if __name__ == '__main__':
    rss_feed_pipeline.serve(
        name="deploy-rss-pipeline",
        tags=["rss", "news"],
        interval=60
    )
