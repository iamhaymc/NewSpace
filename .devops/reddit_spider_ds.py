class AssetRecord:
    def __init__(
        self,
        id=None,
        utc_created=None,
        utc_updated=None,
        media_type=None,
        media_url=None,
        media_data=None,
    ):
        self.id = id
        self.utc_created = utc_created
        self.utc_updated = utc_updated
        self.media_type = media_type
        self.media_url = media_url
        self.media_data = media_data


class UserRecord:
    def __init__(
        self,
        id=None,
        utc_created=None,
        utc_updated=None,
        src_slug=None,
        src_url=None,
        src_data=None,
    ):
        self.id = id
        self.utc_created = utc_created
        self.utc_updated = utc_updated
        self.src_slug = src_slug
        self.src_url = src_url
        self.src_data = src_data


class SpaceRecord:
    def __init__(
        self,
        id=None,
        utc_created=None,
        utc_updated=None,
        src_slug=None,
        src_url=None,
        src_data=None,
    ):
        self.id = id
        self.utc_created = utc_created
        self.utc_updated = utc_updated
        self.src_slug = src_slug
        self.src_url = src_url
        self.src_data = src_data


class PostRecord:
    def __init__(
        self,
        id=None,
        utc_created=None,
        utc_updated=None,
        src_slug=None,
        src_url=None,
        src_data=None,
        space=None,
        title=None,
        author=None,
        type=None,
        text=None,
        score=None,
        parent=None,
    ):
        self.id = id
        self.utc_created = utc_created
        self.utc_updated = utc_updated
        self.src_slug = src_slug
        self.src_url = src_url
        self.src_data = src_data
        self.space = space
        self.title = title
        self.author = author
        self.type = type
        self.text = text
        self.score = score
        self.parent = parent
