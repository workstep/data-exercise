from datetime import datetime

from pony.orm import Database, Required, Set

db = Database()


class LocationIndex(db.Entity):
    """
    Our "dimension table" for "normalized" lat/lng pairs
    """
    _table_ = "location_index"
    latitude = Required(float)
    longitude = Required(float)
    wage_stats = Set("WageStat")


class WageStat(db.Entity):
    """
    Our wage event "fact table," which stores processed rows
    from our raw data
    """
    _table_ = "wage_stat"
    location_index = Required(LocationIndex)
    ctime = Required(datetime)
    wage = Required(float)
    title = Required(str)

# Establish DB connection
db.bind(provider="sqlite", filename="database.sqlite", create_db=True)
db.generate_mapping(create_tables=True)
