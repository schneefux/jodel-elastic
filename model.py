import dateutil
from textblob_de import TextBlobDE
from elasticsearch_dsl import Document, Date, Boolean, \
    Integer, Keyword, Text, Float

def index_name(word):
    return word.lower()\
        .replace("ä", "ae")\
        .replace("ö", "oe")\
        .replace("ü", "ue")\
        .replace("ß", "ss")\
        .replace("/", "_")\
        .replace(" ", "_")

def extract_tags(message):
    return [
        tag.strip("#") for tag in message.split()
        if tag.startswith("#")
    ]

class Reply(Document):
    timestamp = Date(required=True)
    post_id = Keyword(required=True)
    post_timestamp = Date(required=True)

    message = Text(analyzer="german", required=True)
    tags = Keyword(multi=True)
    post_message = Text(analyzer="german", required=True)
    post_tags = Keyword(multi=True)
    color = Keyword(required=True)
    post_color = Keyword(required=True)
    distance = Integer(required=True)
    got_thanks = Boolean(required=True)
    location_name = Keyword(required=True)
    from_home = Boolean()
    vote_count = Integer(required=True)
    replier = Integer(required=True)
    polarity = Float(required=True)

    post_pin_count = Integer(required=True)
    post_share_count = Integer(required=True)
    post_vote_count = Integer(required=True)
    post_polarity = Float(required=True)

    class Index:
        name = "jodel-reply-*-*"
        settings = {
            "number_of_shards": 1
        }

    def from_dict(reply, post):
        blob = TextBlobDE(reply["message"])
        return Reply(
            _id = reply["post_id"],
            timestamp = dateutil.parser.parse(reply["created_at"]),
            post_id = reply["parent_id"],
            post_timestamp = post.timestamp,
            message = reply["message"],
            tags = extract_tags(reply["message"]),
            post_message = post.message,
            post_tags = post.tags,
            color = reply["color"],
            post_color = post.color,
            distance = reply["distance"],
            got_thanks = reply["got_thanks"],
            location_name = reply["location"]["name"],
            from_home = reply.get("from_home"),
            vote_count = reply["vote_count"],
            replier = reply["replier"],
            polarity = blob.polarity,
            post_pin_count = post.pin_count,
            post_share_count = post.share_count,
            post_vote_count = post.vote_count,
            post_polarity = post.polarity
        )

    def save(self, **kwargs):
        time_format = "jodel-reply-" + index_name(self.location_name)\
            + "-%Y%m%d"
        kwargs["index"] = self.timestamp.strftime(time_format)
        return super().save(**kwargs)


class Post(Document):
    timestamp = Date(required=True)

    message = Text(analyzer="german", required=True)
    tags = Keyword(multi=True)
    image_url = Keyword()
    thumbnail_url = Keyword()
    from_home = Boolean()
    child_count = Integer(required=True)
    banned = Boolean(required=True)
    deleted = Boolean(required=True)
    color = Keyword(required=True)
    distance = Integer(required=True)
    location_name = Keyword(required=True)
    pin_count = Integer(required=True)
    share_count = Integer(required=True)
    vote_count = Integer(required=True)
    readonly = Boolean(required=True)
    polarity = Float(required=True)

    class Index:
        name = "jodel-post-*-*"
        settings = {
            "number_of_shards": 1
        }

    def from_dict(details):
        post = details["details"]
        blob = TextBlobDE(post["message"])
        return Post(
            _id = post["post_id"],
            timestamp = dateutil.parser.parse(post["created_at"]),
            message = post["message"],
            tags = extract_tags(post["message"]),
            image_url = post.get("image_url"),
            thumbnail_url = post.get("thumbnail_url"),
            child_count = post["child_count"],
            banned = details["banned"],
            deleted = False,
            from_home = details.get("from_home"),
            color = post["color"],
            distance = post["distance"],
            location_name = post["location"]["name"],
            pin_count = post["pin_count"],
            share_count = post["share_count"],
            vote_count = post["vote_count"],
            readonly = details["readonly"],
            polarity = blob.polarity
        )

    def save(self, **kwargs):
        time_format = "jodel-post-" + index_name(self.location_name)\
            + "-%Y%m%d"
        kwargs["index"] = self.timestamp.strftime(time_format)
        return super().save(**kwargs)


