import requests
import logging
import pandas as pd
from pydantic import BaseModel, ValidationError
from sqlalchemy import create_engine, text
import numpy as np
import os
from dotenv import load_dotenv, dotenv_values
from typing import List, Dict, Tuple, Optional, Any

load_dotenv(dotenv_path="../.env", override=True)

BASE_URL = os.getenv("BASE_URL")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
conn_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@host.docker.internal:5440/{POSTGRES_DB}"


def custom_logger() -> logging.Logger:
    """
    Creates and configures a custom logger for this module.

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(__name__)
    # logger level is set to debug.
    logger.setLevel(logging.DEBUG)
    # Clear existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.propagate = False
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger


logger = custom_logger()


# raw data model
class ComicModel(BaseModel):
    month: int
    num: int
    year: int
    news: str
    safe_title: str
    transcript: str
    alt: str
    img: str
    title: str
    day: int


def latest_comic() -> Optional[int]:
    """
    Fetch the latest comic number from the xkcd API.
    Returns:
        Optional[int]: Latest comic number or None if API fails.
    """
    try:
        response = requests.get(f"{BASE_URL}/info.0.json")
        data = response.json()
        latest_num = int(data["num"])
        logger.info(f"Latest available comic number: {latest_num}")
        return latest_num
    except Exception as e:
        logger.error(f"Error obtaining latest comic:{e}")
        raise


# current_latest = 3167


def fetch_comics(
    start: int, end: int, url: str
) -> Tuple[List[Dict[str, Any]], List[int]]:
    """
    Fetch a range of comics from start to end
    Args:
        start (int): Starting comic number.
        end (int): Ending comic number.
        url (str): Base XKCD URL.

    Returns:
        Tuple[List[dict], List[int]]:
            - list of successful comic JSONs
            - list of invalid comic numbers
    """
    try:
        comic_data = []
        invalid_comic_num = []
        for i in range(start, end + 1):
            response = requests.get(f"{url}/{i}/info.0.json")
            if response.status_code != 404:
                data = response.json()
                comic_data.append(data)
                logger.debug(f"comic {i} successfuly pulled")
            else:
                invalid_comic_num.append(i)
                logger.warning(f"Comic {i} does not exist (404).")
                continue
        return comic_data, invalid_comic_num
    except Exception as e:
        logger.error(f"Error calling API or storing response:{e}")
        raise


def raw_validation(
    raw_data: List[Dict[str, Any]], model: BaseModel
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Schema enforcement of raw comic JSON records using pydantic.
    Args:
        raw_data (List[dict]): Raw comic dictionaries.
        model (BaseModel): Pydantic model to validate
    Returns:
        Tuple[List[dict], List[dict]]:
            - valid records
            - invalid records with errors
    """
    valid = []
    invalid = []

    for raw in raw_data:
        try:
            valid.append(model(**raw).model_dump())
        except ValidationError as e:  # add logging
            logger.error(f"invalid data: {raw},{e}")
            invalid.append({"record": raw, "error": str(e)})
            continue
    logger.info("Completed validation")
    return valid, invalid


def clean_validated(validated_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Clean validated comic data and return a pandas DataFrame.

        Args:
            validated_data (List[dict]): Validated comic dictionaries.
        Returns:
            pd.DataFrame: Cleaned dataframe.
    """
    try:
        df = pd.DataFrame.from_dict(validated_data)
        df = df.replace("", np.nan)

        cols = ["transcript", "alt"]

        df[cols] = (
            df[cols]
            .astype(str)  # ensure everything is a string before .str operations
            .replace(r"[\[\]{}\/]", "", regex=True)
            .replace(r"\n", " ", regex=True)
            .replace(r"/n", " ", regex=True)
            .replace("\n", " ", regex=False)
            .replace(r"\\s", " ", regex=True)
            .replace(r"(?<![.!?])\s{2,}([A-Z])", r". \1", regex=True)
            .replace(r"\s{2,}", " ", regex=True)
            .apply(lambda col: col.str.strip())
        )

        logger.info("Successfuly cleaned data")
        return df

    except Exception as e:
        logger.error(f"Error in clean_validated:{e}")
        raise


def db_insert(df: pd.DataFrame, table: str) -> None:
    """
    Insert a dataframe into a PostgreSQL table.
    Args:
        df (pd.DataFrame): Dataframe to write.
        table (str): Table name.
    """
    try:
        db = create_engine(conn_string)
        conn = db.connect()
        df.to_sql(table, con=conn, if_exists="append", index=False)
        return logger.info("Successfully appended to database")
    except Exception as e:
        logger.error(f"error inserting into db:{e}")
        raise
    finally:
        if conn:
            conn.close()


def initial_extraction() -> None:
    """
    Perform the initial data extraction.
    """
    logger.info("Starting initial extraction...")
    raw_comics = fetch_comics(1, 3168, BASE_URL)  # 3167 is for testing purposes
    validated_comics = raw_validation(raw_comics[0], ComicModel)
    cleaned_df = clean_validated(validated_comics[0])
    db_insert(cleaned_df, "raw_comics")
    logger.info("Initial extraction complete.")


# current latest would be db value and then db_value +1
def fetch_latest_comic() -> None:
    """
    Fetch only comics newer than the database's max comic number.
    """
    logger.info("Fetching latest comics...")
    db = create_engine(conn_string)
    conn = db.connect()
    result = conn.execute(text("SELECT max(num) FROM raw_comics")).fetchone()
    current_latest = result[0]
    api_latest = latest_comic()
    latest_comics = fetch_comics(current_latest + 1, api_latest, BASE_URL)
    validated_comics = raw_validation(latest_comics[0], ComicModel)
    cleaned_df = clean_validated(validated_comics[0])
    logger.info("inserted latest comic")
    db_insert(cleaned_df, "raw_comics")


def polling_function() -> bool:
    """
    Checks if new comics are available compared to database records.
    Returns:
        bool: True if new comics exist.
    """
    api_latest = latest_comic()
    db = create_engine(conn_string)
    conn = db.connect()
    try:
        result = conn.execute(text("SELECT max(num) FROM raw_comics")).fetchone()
        current_db_latest = result[0]
        return current_db_latest < api_latest
    finally:
        conn.close()
