import geocoder
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:docker@localhost:5432/postgres')
g = geocoder.arcgis('Zanzibar')
print(g)
print(g.json)