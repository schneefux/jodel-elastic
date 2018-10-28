import coloredlogs, logging
import random
import asyncio
import jodel_api
from elasticsearch_dsl.connections import connections
from datetime import datetime, timezone

import config
from model import Reply, Post

class CityWatcher():
    city = ""
    watchers_running = set()
    watchers_done = set()
    requests_done = 0

    def __init__(self, lat, lon, city, account_data):
        self.city = city
        self.j = jodel_api.JodelAccount(
            lat=lat, lng=lon, city=city,
            update_location=False,
            **account_data)

    async def _watch_post(self, post_id):
        if post_id in self.watchers_running or \
                post_id in self.watchers_done:
            return
        self.watchers_running.add(post_id)
        sleep = 60 * 10
        post = None
        while True:
            details = self.j.get_post_details_v3(post_id)[1]
            self.requests_done += 1

            if not "details" in details:
                if details["error"] == "post_blocked":
                    logger.warn("%s %s banned", self.city, post_id)
                    if post is not None:
                        post.banned = True
                        post.save()
                    break
                if details["error"] == "post_deleted":
                    logger.warn("%s %s deleted", self.city, post_id)
                    if post is not None:
                        post.deleted = True
                        post.save()
                    break
                logger.warn("%s %s gone: %s", self.city,
                            post_id, details["error"])
                break

            post = Post.from_dict(details)
            was_new = post.save()
            logger.debug("%s %s %s", self.city, post_id,
                          "created" if was_new else "updated")

            for reply_details in details["replies"]:
                Reply.from_dict(reply_details, post).save()

            while details["next"] != None:
                if details["next"]:
                    details = self.j.get_post_details_v3(
                        post_id,
                        skip=details["next"])[1]
                    self.requests_done += 1
                    for reply_details in details["replies"]:
                        Reply.from_dict(reply_details, post).save()

            time_delta = datetime.now(timezone.utc) - post.timestamp
            hours_ago = time_delta.total_seconds() / 60
            points = post.vote_count + post.child_count
            gravity = 1.8
            hotness = (points + 1) / (hours_ago + 2) ** gravity

            if hotness < 0.1:
                logger.info("%s %s died of boredom (%d points," +\
                            " %.0fh old, score: %.2f)",
                            self.city, post_id, points,
                            hours_ago, hotness)
                break
            await asyncio.sleep(sleep)
            sleep += sleep
        self.watchers_running.remove(post_id)
        self.watchers_done.add(post_id)

    async def run(self):
        while True:
            logger.info("%s fetching recents", self.city)
            recents = self.j.get_posts_recent(skip=None,
                                              limit=None,
                                              after=None,
                                              mine=False,
                                              hashtag=None,
                                              channel="Main")[1]
            for post in recents["posts"]:
                post_id = post["post_id"]
                asyncio.ensure_future(self._watch_post(post_id))

            sleep = int(random.randrange(50, 150) / 100 * 60 * 5)
            await asyncio.sleep(sleep)


async def main():
    watchers = []
    logger.info("booting %d watchers", len(config.ACCOUNTS))
    for (lat, lon, city, account) in config.ACCOUNTS:
        watcher = CityWatcher(lat, lon, city, account)
        asyncio.ensure_future(watcher.run())
        watchers.append(watcher)
        await asyncio.sleep(random.randrange(1, 20))

    while True:
        for w in watchers:
            logger.debug("%s %d running, %d finished," +\
                         " %d requests", w.city,
                         len(w.watchers_running),
                         len(w.watchers_done),
                         w.requests_done)
        await asyncio.sleep(60)

logging.getLogger("elasticsearch").setLevel(logging.WARN)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)

connections.create_connection(hosts=[config.ELASTIC_URI])

post_index_template = Post._index.as_template("post")
post_index_template.save()
reply_index_template = Reply._index.as_template("reply")
reply_index_template.save()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
