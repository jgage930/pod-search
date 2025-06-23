from pydantic import BaseModel, ValidationError, model_validator
import feedparser
from prefect import flow, get_run_logger, task
from datetime import datetime
import json
from typing import Any, Optional

from database import DbTable, db_connect
import yaml

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
    __name__ = 'feed_entries'

    title: str
    summary: str
    link: str
    author: Optional[str] = ''
    feed_id: int


@task(log_prints=True)
def setup_db():
    logger = get_run_logger()
    conn = db_connect()

    FeedTable.create_table(conn, 'feeds')
    FeedEntryTable.create_table(conn, 'feed_entries')


@flow()
def rss_feed_pipeline(url: str, name: str):
    logger = get_run_logger()
    setup_db()
    conn = db_connect()

    test_feed = Feed(
        name=name,
        url=url
    )

    entries = parse_feed(test_feed.url)
    logger.info(f'Read {len(entries)} entries from rss feed.')

    feed_id = FeedTable(**test_feed.model_dump()).insert(conn)
    logger.info(f'Inserted Feed {feed_id}')

    for entry in entries:
        entry_id = FeedEntryTable(**entry.model_dump(), feed_id=feed_id).insert(conn)
        logger.info(f'Inserted Entry {entry_id}')


@flow
def podcasts_pipeline():
    logger = get_run_logger()

    with open('deployments/podcasts.yaml', 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)

    for podcast in config:
        logger.info(f'Pulling podcasts for {podcast['name']}')
        rss_feed_pipeline(podcast['url'], podcast['name'])


if __name__ == '__main__':
    podcasts_pipeline.serve(
        name='Deploy-Podcasts',
        tags=["rss", 'podcast'],
        cron="0 * * * *"
    )