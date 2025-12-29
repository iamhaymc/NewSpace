import os, sys
from pathlib import Path
from omegaconf import OmegaConf as Conf

from reddit_spider_db import *
from reddit_spider_op import *
from reddit_spider_util import *

# =================================================================================================

__file_path__ = Path(__file__)
__dir_path__ = __file_path__.parent

# =================================================================================================


def main():
    # Load configuration
    app_cfg = Conf.from_cli(sys.argv)

    app_cfg_file = __dir_path__.joinpath(f"{__file_path__.stem}.yml")
    if app_cfg_file.exists():
        app_cfg = Conf.merge(app_cfg, Conf.load(app_cfg_file))
    cfg_settings_file = __dir_path__.joinpath(f"cfg.settings.yml")
    if cfg_settings_file.exists():
        app_cfg = Conf.merge(app_cfg, Conf.load(cfg_settings_file))
    cfg_secrets_file = __dir_path__.joinpath(f"cfg.secrets.yml")
    if cfg_secrets_file.exists():
        app_cfg = Conf.merge(app_cfg, Conf.load(cfg_secrets_file))

    # Verify configuration
    app_cfg.app_dir = Path(app_cfg.dir or __dir_path__)
    app_cfg.data_dir = Path(
        app_cfg.data_dir if "data_dir" in app_cfg else __dir_path__.joinpath(f"data")
    )
    app_cfg.cache_dir = Path(
        app_cfg.cache_dir if "cache_dir" in app_cfg else __dir_path__.joinpath(f"cache")
    )
    app_cfg.db_file = Path(
        app_cfg.db_file
        if "db_file" in app_cfg
        else __file_path__.joinpath(f"{__file_path__.stem}.db")
    )

    # Crawl Targets
    try:
        db_con = connect_database(app_cfg.db_file)
        db_cur = db_con.cursor()

        for space_cfg in app_cfg.spaces:
            print(f"Crawling target: {space_cfg.name}")
            try:
                crawl_space(space_cfg, app_cfg, db_cur)
            except Exception as e:
                print(f"Crawling error: {e}")

    finally:
        if db_cur:
            db_cur.close()
        if db_con:
            db_con.commit()
            db_con.close()


if __name__ == "__main__":
    main()
