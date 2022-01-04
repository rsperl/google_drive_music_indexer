#!/usr/bin/env python3

from __future__ import print_function

import json
import logging
import os
import sys
from typing import Union

import gspread
import yaml
from google.oauth2 import service_account
from googleapiclient import discovery
from sqlalchemy import Column, String
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata",
    "https://www.googleapis.com/auth/drive",
]
# Get a client using creds in json from google console
# Docs on how to get creds here: https://pygsheets.readthedocs.io/en/stable/authorizing.html
INCLUDE_FIELDS = "nextPageToken, files(id, name, mimeType, parents, webViewLink)"
DEFAULT_FIND_PAGE_SIZE = 1000

FOLDER_CACHE = {}
MIME_TYPE_FOLDER = "application/vnd.google-apps.folder"

SCHEMA = [
    """
    CREATE TABLE songs (
    artist      STRING COLLATE NOCASE,
    name        STRING COLLATE NOCASE,
    instrument  STRING COLLATE NOCASE,
    location    STRING,
    link, STRING,
    document_id STRING PRIMARY KEY
);
    """
]
Base = declarative_base()


class Song(Base):
    __tablename__ = "songs"
    document_id = Column(String, primary_key=True)
    artist = Column(String)
    name = Column(String)
    location = Column(String)
    instrument = Column(String)
    link = Column(String)


def setup_logging(debug=False, kvSeparator="", always_include_separator=False):
    # https://docs.python.org/3/library/logging.html
    level = logging.INFO
    if debug:
        level = logging.DEBUG

    fmt = "%(asctime)s %(levelname)-7s %(name)s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s %(kv)s"

    # log to stderr
    handler = logging.StreamHandler(stream=sys.stderr)

    prefix = kvSeparator.strip() + " " if kvSeparator else ""

    class AddKeyValueFilter(logging.Filter):
        def filter(self, record):
            try:
                record.kv = prefix + json.dumps(record.kv, sort_keys=True)
            except:  # noqa
                if always_include_separator:
                    setattr(record, "kv", prefix)
                else:
                    setattr(record, "kv", "")
            return True

    # with a key value filter added
    handler.addFilter(AddKeyValueFilter())

    # logging.basicConfig(handlers=[handler], format=fmt)
    logging.basicConfig(handlers=[handler], format=fmt)
    for name in [
        "googleapiclient",
        "google_auth_httplib2",
        "urllib3.connectionpool",
        "urllib3.util.retry",
        "google.auth.transport.requests",
    ]:
        logging.getLogger(name).setLevel(logging.CRITICAL)
    logging.getLogger().setLevel(level)


def initialize_database(session: Session, filename: str = "data.dat"):
    if os.path.exists(filename):
        os.unlink(filename)
    session = create_engine("sqlite:///" + filename)
    for line in SCHEMA:
        session.execute(line)


def load_songs(drive_svc, index_paths, dbsession: Session):
    logger = logging.getLogger(__name__)
    all_songs = {}
    for path in index_paths:
        path_id = path.get("id")
        path_name = path.get("name")
        artists = get_folders(drive_svc, path_id)
        for artist in artists:
            artist_name = artist.get("name")
            instruments_folders = get_files(drive_svc, artist["id"])
            for instrument_folder in instruments_folders:
                instrument = instrument_folder.get("name")
                if instrument in ["guitar", "ukulele"]:
                    songs = get_files(drive_svc, instrument_folder["id"])
                    for song in songs:
                        song_name = song.get("name")
                        song_doc_id = song.get("id")
                        song_link = song.get("webViewLink")
                        song_path_name = "/".join([path_name, artist_name, instrument])
                        logger.info(
                            f"found {artist_name}: {song_name} ({instrument} in {song_path_name})"
                        )
                        s = Song(
                            document_id=song_doc_id,
                            artist=artist_name,
                            name=song_name,
                            instrument=instrument,
                            location=song_path_name,
                            link=song_link,
                        )
                        dbsession.merge(s)
                        all_songs[song_doc_id] = s
    dbsession.commit()
    return all_songs


def setup_worksheet(sheet: gspread.Worksheet):
    logger = logging.getLogger(__name__)  # noqa
    logger.info("setup worksheet")
    sheet.clear()
    headers = ["Artist", "Name", "Instrument", "Location", "Document ID"]
    rows = [headers]
    sheet.append_rows(rows)
    sheet.freeze(rows=1)
    sheet.set_basic_filter()
    sheet.format("A1:M1", {"textFormat": {"bold": True}})


def load_spreadsheet(sheet: gspread.Worksheet, dbsession: Session):
    logger = logging.getLogger(__name__)
    row_number = 2
    for song in (
        dbsession.query(Song).order_by(Song.artist, Song.name, Song.instrument).all()
    ):
        logger.info(f"adding {song.artist}: {song.name} ({song.instrument})")

        name_href = f'=HYPERLINK("{song.link}", "{song.name}")'
        row = [song.artist, song.name, song.instrument, song.location, song.document_id]
        sheet.append_row(row)
        sheet.update_cell(row_number, 2, name_href)
        row_number += 1
    sheet.columns_auto_resize(1, 10)


def get_files(service, artist_folder_id, find_page_size=DEFAULT_FIND_PAGE_SIZE):
    """
    :param service: google drive service
    :param artist_folder_id: folder id of the service
    :return: list of dicts of file objects
    """
    results = (
        service.files()
        .list(
            pageSize=find_page_size,
            q=f"'{artist_folder_id}' in parents",
            fields=INCLUDE_FIELDS,
        )
        .execute()
    )
    return results.get("files", [])


def read_config(config_file: str) -> dict:
    with open(config_file, "r") as fh:
        return yaml.load(fh, yaml.Loader)


def get_folders(service, parent_folder_id, find_page_size=DEFAULT_FIND_PAGE_SIZE):
    """
    :param service: google service
    :param parent_folder_id:  id of folder containing artist folders
    :return: list of dicts of folder objects
    """
    results = (
        service.files()
        .list(
            pageSize=find_page_size,
            q=f"mimeType = 'application/vnd.google-apps.folder' and '{parent_folder_id}' in parents",
            fields=INCLUDE_FIELDS,
        )
        .execute()
    )
    return results.get("files", [])


def connect_to_database(dbname: str = "data.dat") -> Session:
    if os.path.exists(dbname):
        os.unlink(dbname)
    eng = create_engine("sqlite:///" + dbname)
    Session = sessionmaker()
    Session.configure(bind=eng)
    return Session()


def main():
    setup_logging(True)
    logger = logging.getLogger(__name__)

    # load configuration and credentials
    config_file = "./.music_indexer.yaml"
    service_account_file = os.getenv("MUSIC_INDEXER_CLIENT_CREDS")
    config = read_config(config_file)
    spreadsheet_id = config.get("spreadsheet_id")
    sheet_name = config.get("sheet_name")

    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=SCOPES
    )

    # connect to drive
    drive_svc = discovery.build("drive", "v3", credentials=credentials)

    # connect to sheets
    gc = gspread.service_account(filename=service_account_file)

    # connect to database
    dbsession = connect_to_database()

    workbook = gc.open_by_key(spreadsheet_id)
    worksheet = None
    for s in workbook.worksheets():
        if s.title == sheet_name:
            worksheet = s

    if not worksheet:
        sys.exit(f"did not find sheet in workbook: {sheet_name}")

    setup_worksheet(worksheet)
    initialize_database(dbsession)
    index_paths = config.get("index_paths")
    load_songs(drive_svc, index_paths, dbsession)
    load_spreadsheet(worksheet, dbsession)


if __name__ == "__main__":
    main()
