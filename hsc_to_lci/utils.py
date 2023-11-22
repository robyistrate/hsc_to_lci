"""
Various utils functions.
"""

import os
import pandas as pd
import yaml
import copy
import brightway2 as bw
import bw2io
import wurst
from constructive_geometries import *

from . import __version__, DATA_DIR
from pathlib import Path


ECOINVENT_UNITS = DATA_DIR / "export" / "ecoinvent_units.yaml"
GASES_PROPERTIES = DATA_DIR / "export" / "gases_properties.yaml"


def open_bw_project(bw_proj: str):
    bw.projects.set_current(bw_proj)


def import_simulation_results(filepath: str):
    """
    Import simulation results from a spreadsheet file
    :param filepath:
    :return: DataFrame object
    """
    return pd.ExcelFile(filepath)


def import_ecoinvent_db(source_db: str):
    """
    Import the ecoinvent database into wurst format
    """
    print("Importing the ecoinvent database...")
    return wurst.extract_brightway2_databases(source_db)


def import_biosphere_db():
    print("Importing the biosphere database...")
    return [ef.as_dict() for ef in bw.Database('biosphere3')]


def get_ecoinvent_units():
    with open(ECOINVENT_UNITS, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


def get_simulation_lci_map(filepath: str):
    """
    Import mapping between simulation stream names and LCI flows
    :param filepath:
    :return: dict
    """
    df= pd.read_excel(filepath, index_col=0)
    return {index: row.to_dict() for index, row in df.iterrows()}


def get_gases_properties():
    with open(GASES_PROPERTIES, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data


def get_dataset_code():
    return wurst.filesystem.get_uuid()


def units_conversion(df):
    """
    Convert units into ecoinvent requirements
    :param df: DataFrame containing process simulation data
    :return: DataFrame object with adjusted units
    """

    convert_kg_to_cum = ['natural gas',
                         "air",
                         "h2o(g)"]

    gases_properties = get_gases_properties()

    for index, row in df.iterrows():

        if row["Stream Name"].lower() in convert_kg_to_cum:
            if row["Unit"] == "cubic meter":
                pass
            else:
                if row["Unit"] == "kilogram":
                    row['Amount'] = row['Amount'] / gases_properties[row["Stream Name"].lower()]["density"]
                row['Unit'] = "cubic meter"

    return df


def get_production_flow_exchange(ds: dict):
    """
    Convert units into ecoinvent requirements
    :param ds: dictionary containing the dataset information
    :return: dictionary containing the production exchange
    """
    return {'name': ds['name'],
            'product': ds['reference product'],
            'location': ds['location'],
            'amount': ds['production amount'],
            'unit': ds['unit'],
            'database': ds['database'],
            'type': 'production',
            "input": (ds["database"], ds["code"])
           }


def get_dataset_for_location(loc: str, exc_filter: dict, ei_db: list):
    """
    Find new technosphere suppliers for the provided location.
    Based on 'wurst.transformations.geo.relink_technosphere_exchanges'

    :param loc: string representing the target location
    :param exc_filter: dictionary containing the name, reference product, and unit for the activity
    :param ei_db: list of dictionaries containing ecoinvent inventories
    :return: dictionary containing the dataset
    """
    geomatcher = Geomatcher() # Initialize the geomatcher object

    # Get all possible datasets for all locations; get both "market group" and "market" activities
    if 'market for' in exc_filter['name']:
        possible_datasets_market = list(wurst.transformations.geo.get_possibles(exc_filter, ei_db)) 

        exc_filter_market = copy.deepcopy(exc_filter)
        exc_filter_market.update({'name': exc_filter['name'].replace('market', 'market group')})
        possible_datasets_market_group = list(wurst.transformations.geo.get_possibles(exc_filter_market, ei_db))

        possible_datasets = possible_datasets_market + possible_datasets_market_group
    else:
        possible_datasets = list(wurst.transformations.geo.get_possibles(exc_filter, ei_db))

    # Check if there is an exact match for the target location
    match_dataset = [ds for ds in possible_datasets if ds['location'] == loc]

    # If there is no specific dataset for the target location, search for the supraregional locations
    if len(match_dataset) == 0:
        loc_intersection = geomatcher.intersects(loc, biggest_first=False)
        loc_intersection = [i[1] if type(i)==tuple else i for i in loc_intersection]
        loc_intersection.insert(loc_intersection.index("GLO"), "RoW") # Inser RoW before GLO

        for loc in loc_intersection:
            match_dataset = [ds for ds in possible_datasets if ds['location'] == loc]
            if len(match_dataset) > 0:
                break

    return match_dataset[0]


def link_exchanges_by_code(inventories: list, ei_db: list, bio_db: list):
    '''
    This function links in place technosphere exchanges within the database and/or to an external database
    and biosphere exchanges with the biosphere database (only unlinked exchanges)
    
    :param inventories: list of dictionaries containing inventories
    :param ei_db: list of dictionaries containing ecoinvent inventories
    :param bio_db: list of dictionaries containing biosphere flows metadata
    '''   
    technosphere = lambda x: x["type"] == "technosphere"
    biosphere = lambda x: x["type"] == "biosphere"
    
    for ds in inventories:
        
        for exc in filter(technosphere, ds["exchanges"]):
            if 'input' not in exc:
                try:
                    exc_lci = wurst.get_one(inventories + ei_db,
                                            wurst.equals("name", exc['name']),
                                            wurst.equals("reference product", exc['product']),
                                            wurst.equals("location", exc['location'])
                                        )
                    exc.update({'input': (exc_lci['database'], exc_lci['code'])})
                except Exception:
                    print(exc['name'], exc['product'], exc['location'])
                    raise
            
        for exc in filter(biosphere, ds["exchanges"]):
            if 'input' not in exc:
                try:
                    ef_code = [ef['code'] for ef in bio_db if ef['name'] == exc['name'] and 
                                                              ef['unit'] == exc['unit'] and 
                                                              ef['categories'] == exc['categories']][0]
                    exc.update({'input': ('biosphere3', ef_code)})   
                except Exception:
                    print(exc['name'], exc['unit'], exc['categories'])
                    raise


def load_project_metadata(filepath: str) -> dict:
    """
    Load the metadata of the project.
    :param filepath:
    :return: metadata
    """
    # read YAML file
    with open(filepath, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return data


def write_db_to_bw(inventories: list, db_name: str):
    """
    Write database to Brightway2 and export inventory in Excel

    :param inventories: list of dictionary each containing a dataset
    :param: db_name: name of the new database
    """    
    if db_name in bw.databases:
        del bw.databases[db_name]
    wurst.write_brightway2_database(inventories, db_name)

    return bw2io.export.excel.write_lci_excel(db_name)
    



