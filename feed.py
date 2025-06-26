from pydantic import BaseModel, ValidationError, model_validator
import feedparser
from prefect import flow, get_run_logger, task
from datetime import datetime
import json
from typing import Any, Optional

# from database import DbTable, db_connect
import database as db
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


class FeedTable(db.DbTable):
    __table_name__ = 'feeds'

    name: str
    url: str


class FeedEntryTable(db.DbTable):
    __table_name__ = 'feed_entries'

    title: str
    summary: str
    link: str
    author: Optional[str] = ''
    feed_id: int


@task(log_prints=True)
def setup_db():
    conn = db.db_connect()

    FeedTable.create_table(conn, 'feeds')
    FeedEntryTable.create_table(conn, 'feed_entries')


@flow()
def rss_feed_pipeline(url: str, name: str):
    logger = get_run_logger()
    setup_db()
    conn = db.db_connect()
    
    feed_in_db = db.select(conn, FeedTable, {'url': url})
    feed_id = 0
    if feed_in_db:
        logger.info(f'Feed url found in db with name {feed_in_db[0].name}.')
        feed_id = feed_in_db[0].id
    else:
        logger.info('Adding feed to db...')
        feed_id = db.insert(conn, FeedTable(name=name, url=url))

    logger.info('Parsing rss feed...')
    entries = parse_feed(url)
    logger.info(f'Read {len(entries)} entries from rss feed.')

    logger.info('Dropping entries from db...')
    db.delete(conn, FeedEntryTable, {'feed_id': feed_id})

    entries = [FeedEntryTable(**entry.model_dump(), feed_id=feed_id) for entry in entries]
    db.bulk_insert(conn, entries)


@flow
def podcasts_pipeline():
    logger = get_run_logger()

    with open('deployments/podcasts.yaml', 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)

    for podcast in config:
        logger.info(f'Pulling podcasts for {podcast['name']}')
        rss_feed_pipeline(podcast['url'], podcast['name'])


if __name__ == '__main__':
    # podcasts_pipeline.serve(
    #     name='Deploy-Podcasts',
    #     tags=["rss", 'podcast'],
    #     cron="0 * * * *"
    # )
    rss_feed_pipeline('https://feeds.libsyn.com/65267/rss', 'Lore')