# 1. Import necessary libraries
from datetime import datetime, date
import pandas as pd

from neo4j.time import Date


# 2. Functions
def convert_neo4j_date(x):
    """
    Aux function used to convert a neo4j date type column into a datetime type of column

    Parameters:
        - x: The desired date to modify.

    Returns:
        - x: The date with its new format.
    """
    if isinstance(x, Date):
        return date(x.year, x.month, x.day)
    elif isinstance(x, datetime):
        return x.date()
    elif isinstance(x, date):
        return x
    else:
        return pd.NaT
