from reddit_spider_util import *
from reddit_spider_db import *

# =================================================================================================

BASE_URL_NEW = f"https://reddit.com"
BASE_URL_OLD = f"https://old.reddit.com"
BASE_URL_OLD2 = "https://www.reddit.com/domain/old.reddit.com"

DATA_KIND_COMMENT = "t1"
DATA_KIND_AUTHOR = "t2"
DATA_KIND_SUBMISSION = "t3"
DATA_KIND_SPACE = "t5"

QUERY_ITEM_SORT = "top"  # Options: top, hot, new
QUERY_ITEM_TIME = "all"  # Options: all, year, month
QUERY_ITEM_LIMIT = 100  # Integer in range [25, 100]

SUPPORTED_IMAGES_EXTENSIONS = [".png", ".jpg", ".jpeg", ".webp"]

CRAWL_MEDIA_IMAGE_GALLERIES = True
CRAWL_MEDIA_UNANIMATED_IMAGES = True
CRAWL_MEDIA_ANIMATED_IMAGES = False
CRAWL_MEDIA_VIDEOS = False


def _fetch_user_post_data(
    username, limit=QUERY_ITEM_LIMIT, sort=QUERY_ITEM_SORT, after=None
):
    url = f"{BASE_URL_OLD}/user/{username}/submitted"
    url += "/.json?raw_json=1"
    if after:
        url += f"&after={after}"
    if limit:
        url += f"&limit={max(25, min(100, limit))}"
    if sort:
        url += f"&sort={sort}"
    return fetch_json(url)


def _fetch_space_post_data(
    space_name,
    limit=QUERY_ITEM_LIMIT,
    time=QUERY_ITEM_TIME,
    sort=QUERY_ITEM_SORT,
    after=None,
):
    url = f"{BASE_URL_OLD}/r/{space_name}"
    if sort:
        url += f"/{sort}"
    url += "/.json?raw_json=1"
    if after:
        url += f"&after={after}"
    if limit:
        url += f"&limit={max(25, min(100, limit))}"
    if time:
        url += f"&t={time}"
    return fetch_json(url)


# =================================================================================================


def _resolve_post_media(post):
    out_items = []
    media_domain = post["domain"]

    if CRAWL_MEDIA_UNANIMATED_IMAGES and media_domain == "i.redd.it":
        media_url = post["url"]
        out_items.append({"url": media_url, "mime": get_path_mime(media_url)})

    elif CRAWL_MEDIA_ANIMATED_IMAGES and media_domain == "gfycat.com":
        id = post["url"].split("/")[-1]
        media_info_url = f"https://gfycat.com/cajax/get/{id}"
        media_info = fetch_json(media_info_url, assert_status=False)
        if media_info:
            media_url = media_info["gfyItem"]["mp4Url"]
            out_items.append({"url": media_url, "mime": get_path_mime(media_url)})

    elif CRAWL_MEDIA_ANIMATED_IMAGES and media_domain == "redgifs.com":
        id = post["url"].split("/")[-1]
        media_info_url = f"https://api.redgifs.com/v1/gfycats/{id}"
        media_info = fetch_json(media_info_url, assert_status=False)
        if media_info:
            media_url = media_info["gfyItem"]["mp4Url"]
            out_items.append({"url": media_url, "mime": get_path_mime(media_url)})

    elif CRAWL_MEDIA_VIDEOS and media_domain == "v3.redgifs.com":
        media_url_og = post["url_overridden_by_dest"]
        media_id_re = re.findall(".*/watch/(.+)#?", media_url_og)
        if media_id_re:
            media_info_url = f"https://api.redgifs.com/v1/gfycats/{media_id_re[0]}"
            media_info = fetch_json(media_info_url, assert_status=False)
            if media_info:
                media_url = media_info["gfyItem"]["mp4Url"]
                out_items.append({"url": media_url, "mime": get_path_mime(media_url)})
        else:
            print(f"Failure to determine media id from url: {media_url_og}")

    elif CRAWL_MEDIA_VIDEOS and media_domain == "i.imgur.com":
        media_url = post["url"].replace(".gifv", ".mp4")
        out_items.append({"url": media_url, "mime": get_path_mime(media_url)})

    elif CRAWL_MEDIA_IMAGE_GALLERIES and "gallery_data" in post:
        gallery_data = post["gallery_data"]
        if gallery_data:
            media_meta = post["media_metadata"]
            ids = [x["media_id"] for x in gallery_data["items"]]
            media_urls = []
            for id in ids:
                entry = media_meta[id]
                if "m" in entry:
                    media_mime = entry["m"]
                    if media_mime == "image/gif":
                        media_urls.append(entry["s"]["gif"])
                    else:
                        media_urls.append(entry["s"]["u"])
                else:
                    print("image in gallery might be unprocessed")
            out_items.extend([{"url": x, "mime": get_path_mime(x)} for x in media_urls])

    if not out_items:
        if media_domain.startswith("self."):
            print(f"Warning: no media identified due to domain {media_domain}")
        else:
            print(f"Warning: no media identified")

    return out_items


def _map_submission_item(item):
    model = SimpleNamespace()
    model.kind = DATA_KIND_SUBMISSION
    model.src_data = item
    model.src_url = f'{BASE_URL_OLD}{item["permalink"]}'
    model.id = item["name"]
    model.title = item["title"]
    model.date_utc = datetime.fromtimestamp(item["created_utc"]).isoformat()
    model.sub_id = item["subreddit_id"]
    model.sub_name = item["subreddit"]
    model.author_id = item["author_fullname"] if "author_fullname" in item else None
    model.author_name = item["author"] if "author" in item else None
    model.score_ups = item["ups"]
    model.score_downs = item["downs"]
    model.text = item["selftext"]
    model.media_items = _resolve_post_media(item)
    return model


def _map_comment_item(item):
    model = SimpleNamespace()
    model.kind = DATA_KIND_COMMENT
    model.src_data = item
    model.src_url = item["permalink"]
    model.id = item["name"]
    model.date_utc = datetime.fromtimestamp(item["created_utc"]).isoformat()
    model.sub_id = item["subreddit_id"]
    model.sub_name = item["subreddit"]
    model.author_id = item["author_fullname"] if "author_fullname" in item else None
    model.author_name = item["author"] if "author" in item else None
    model.score_ups = item["ups"]
    model.score_downs = item["downs"]
    model.text = item["body"]
    return model


def _iterate_posts(fetch_cb):
    items = []
    next_post, page_index = None, 0
    while True:
        print(f'(Page #{page_index} (after post "{next_post or "<none>"}")')
        page_data = fetch_cb(next_post)
        page_data = page_data if isinstance(page_data, list) else [page_data]
        for listing in page_data:
            children = listing["data"]["children"]
            items.extend(
                [
                    _map_submission_item(x["data"])
                    for x in children
                    if x["kind"] == DATA_KIND_SUBMISSION
                ]
            )
            items.extend(
                [
                    _map_comment_item(x["data"])
                    for x in children
                    if x["kind"] == DATA_KIND_COMMENT
                ]
            )
        next_post = page_data[0]["data"]["after"]
        if next_post:
            page_index += 1
        else:
            break
    return items


def get_user_posts(user_name):
    return _iterate_posts(
        lambda next_post: _fetch_user_post_data(user_name, after=next_post)
    )


def get_space_posts(space_name):
    return _iterate_posts(
        lambda next_post: _fetch_space_post_data(space_name, after=next_post)
    )


# =================================================================================================


def crawl_space(space_cfg, app_cfg, db_cur):

    # Insert space record
    space_record = SpaceRecord(name=space_cfg.name)
    insert_space(db_cur, space_record)

    post_items = list(get_space_posts(space_cfg.name))
    for post in post_items:

        # Insert post record
        post_rec = PostRecord()
        post_rec.name = post.name
        post_rec.src_url = post.src_url
        post_rec.kind = post.kind
        post_rec.title = post.title
        post_rec.date_utc = post.date_utc
        post_rec.sub_id = post.sub_id
        post_rec.sub_name = post.sub_name
        post_rec.author_id = post.author_id
        post_rec.author_name = post.author_name
        post_rec.score_ups = post.score_ups
        post_rec.score_downs = post.score_downs
        post_rec.text = post.text
        insert_post(db_cur, post_rec)

        # Handle post assets
        img_dir = app_cfg.cache_dir.joinpath("images")
        img_dir.mkdir(parents=True, exist_ok=True)

        for i, media_item in enumerate(post.media_items):
            ext = mimetypes.guess_extension(media_item["mime"])

            if ext in SUPPORTED_IMAGES_EXTENSIONS:
                img_name = f"{post['id']}_{i}{ext}"
                img_file = img_dir.joinpath(
                    f"{post['author_name']}-{post['author_id']}-{img_name}"
                )
                print(f'Downloading image: "{img_name}"')
                fetch_file(media_item["url"], img_file)
