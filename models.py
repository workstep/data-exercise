from pony.orm import Database, Required, Optional

db = Database()

class BenchmarkLocation(db.Entity):
    _table_ = 'benchmark_location'
    latitude = Required(float)
    longitude = Required(float)

db.bind(provider='sqlite', filename='database.sqlite', create_db=True)
db.generate_mapping(create_tables=True)
