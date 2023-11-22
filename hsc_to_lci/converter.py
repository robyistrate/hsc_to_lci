
from .utils import (
    import_simulation_results,
    open_bw_project,
    import_ecoinvent_db,
    import_biosphere_db,
    get_ecoinvent_units,
    get_simulation_lci_map,
    units_conversion,
    get_dataset_code,
    get_production_flow_exchange,
    get_dataset_for_location,
    link_exchanges_by_code,
    load_project_metadata,
    write_db_to_bw
)

from pathlib import Path
import pandas as pd
import numpy as np
import math
import datetime
import shutil


class Converter:
    """
    Convert HSC Chemistry results to Brightway2 inventories.
    """

    def __init__(
            self,
            metadata: str = None,
            export_dir: str=None
    ):
        self.metadata = load_project_metadata(metadata)

        self.db_name = self.metadata['system description']['database']
        self.bw_proj = self.metadata['brightway project']['project name']
        self.source_db = self.metadata['brightway project']['ecoinvent database']
        open_bw_project(self.bw_proj)

        self.simulation_file = self.metadata['input files']['simulation file']
        self.mapping_file = self.metadata['input files']['mapping file']

        self.simulation_results = import_simulation_results(self.simulation_file)
        self.ecoinvent_db = import_ecoinvent_db(self.source_db)
        self.biosphere_db = import_biosphere_db()
        self.ecoinvent_units = get_ecoinvent_units()
        self.simulation_lci_map = get_simulation_lci_map(self.mapping_file)

        # export directory is the current working
        # directory unless specified otherwise
        if export_dir:
            self.export_dir = Path(export_dir)
        else:
            self.export_dir =  Path.cwd()

    
    def get_input_output_streams_data(self):
        """
        Extract input and output streams data.

        - Input streams are all considered to be input products from the technosphere
        - Output streams can be emissions to the environmental or waste streams (which are technosphere flows)

        :return: dataframe containing the input or output streams data for each unit process
        """

        print("Extract process simulation data")

        technosphere_flows = [i for i in self.simulation_lci_map
                              if self.simulation_lci_map[i]["LCI flow type"] == "technosphere"]
        biosphere_flows = [i for i in self.simulation_lci_map
                           if self.simulation_lci_map[i]["LCI flow type"] == "biosphere"]        
        biosphere_air_flows = [i for i in self.simulation_lci_map
                               if self.simulation_lci_map[i]["LCI flow type"] == "biosphere"
                               and self.simulation_lci_map[i]["Category"] == "air"]

        input_output_data = pd.DataFrame()

        for sheet in ['Input Streams', "Output Streams"]:
            df = self.simulation_results.parse(sheet)

            new_header = df.iloc[0]
            df.columns = new_header
            df = df[1:]
            df.columns.values[1] = "Stream Properties"
            df.columns.values[4] = "Stream Property Amount"
            df = df.drop(columns=["Use Exergy",
                                  "LCA Equivalent",
                                  "LCA Group",
                                  "Main Product"]
                                  )

            # For input streams, only keep the "Stream Name"
            if sheet == "Input Streams":
                df = df.dropna(subset="Stream Name").dropna(axis=1, how='all')
                df = df.set_index('Unit Name').sort_index(axis=0)

            # For output streams, compute and keep the Stream Properties
            # Only if the Stream Property is included in the LCI mapping list
            elif sheet == "Output Streams":
                df['Stream Name'].fillna(method='ffill', inplace=True)
                df['Unit Name'].fillna(method='ffill', inplace=True)

                output_data = []
                for index, row in df.iterrows():
                        
                    # Emissions to air
                    if row["Stream Properties"] in biosphere_air_flows:

                        property_amount = float(df.loc[index]["Stream Property Amount"].replace(',', '.'))
                        if property_amount == 0:
                            pass
                        else:
                            # Get stream data
                            stream_data = df[(df["Stream Name"] == row["Stream Name"]) 
                                             & (df["Unit Name"] == row["Unit Name"])]

                            stream_amount = stream_data[~stream_data['Amount'].isna()]["Amount"].values[0]
                            stream_unit = stream_data[~stream_data['Unit'].isna()]["Unit"].values[0]
                            stream_mass_flow = float(stream_data[stream_data["Stream Properties"] == "Mass Flow"]["Stream Property Amount"].values[0].replace(',', '.'))
                                
                            # Compute LCI amount:
                            lci_amount = property_amount / stream_mass_flow * stream_amount

                            output_stream_lci = {
                                        "Unit Name": row["Unit Name"],
                                        "Stream Name": row["Stream Properties"],
                                        "Amount": lci_amount,
                                        "Unit": stream_unit
                                        }

                            output_data.append(output_stream_lci)

                df = pd.DataFrame(output_data)
                df = df.set_index('Unit Name').sort_index(axis=0)

            df['Stream type'] = sheet # add stream type (input or output)

            input_output_data = pd.concat([input_output_data, df])
            input_output_data = input_output_data.sort_index()

        print("Apply strategies:")

        print("... add technosphere/biosphere flow type")
        input_output_data['LCI type'] = np.where(input_output_data['Stream Name'].isin(technosphere_flows), 'technosphere',
                                        np.where(input_output_data['Stream Name'].isin(biosphere_flows), 'biosphere',
                                        None)
                                        )
    
        print("... change units to ecoinvent format")
        input_output_data['Unit'] = input_output_data['Unit'].apply(lambda x: self.ecoinvent_units.get(x, x))

        print("... convert process simulation units to ecoinvent units")
        input_output_data = units_conversion(input_output_data)

        return input_output_data


    def format_inventories_for_bw(self):
        """
        Format inventories to Brightway2 format.
        :return: list of dictionaries; each dictionary represents a unit process inventory
        """

        print("Format inventories to Brightway2 format")

        activity_name = self.metadata['system description']['activity name']
        activity_reference_product = self.metadata['system description']['reference product']
        activity_location = self.metadata['system description']['location']
        activity_comment = self.metadata['system description']['comment']

        input_output_streams_data = self.get_input_output_streams_data()

        # List of unit processes
        unit_processes = list(set(input_output_streams_data.index))
        
        inventories = []

        # Create inventory dicts for each unit process
        # !!! Each unit process produces "1 unit" of the unit process
        for up in unit_processes:
            up_dict = {
                       'name': activity_name + ", " + up,
                       'reference product': activity_reference_product + ", " + up,
                       'location': activity_location,
                       "production amount": 1,
                       'unit': "unit",
                       "database": self.db_name,
                       "code": get_dataset_code(),
                       "comment": activity_comment
                       }
            
            # Add exchanges to the inventory
            exchanges = []

            # Add production flow to exchanges
            exchanges.append(get_production_flow_exchange(up_dict)) 

            for index, row in input_output_streams_data.loc[[up]].iterrows():

                if row["LCI type"] == "technosphere":

                    exc_filter = {
                        'name': self.simulation_lci_map[row["Stream Name"]]["Name"],
                        'product': self.simulation_lci_map[row["Stream Name"]]["Reference product"],
                        'unit': row['Unit']
                    }

                    exc_dataset = get_dataset_for_location(activity_location,
                                                           exc_filter,
                                                           self.ecoinvent_db)
                    
                    exchanges.append(
                                        {
                                        'name': exc_dataset["name"],
                                        "product": exc_dataset["reference product"],
                                        'location': exc_dataset["location"],
                                        "amount": row['Amount'],
                                        "unit": row['Unit'],
                                        'database': self.source_db,
                                        'type': "technosphere"
                                        }
                        )

                elif row["LCI type"] == "biosphere":
                    categories = (
                            self.simulation_lci_map[row["Stream Name"]]["Category"],
                            self.simulation_lci_map[row["Stream Name"]]["Subcategory"]
                        )
                    if isinstance(categories[1], float) and math.isnan(categories[1]):
                        categories = (categories[0],)

                    exchanges.append(
                        {'name': self.simulation_lci_map[row["Stream Name"]]["Name"],
                         'amount': row['Amount'],
                         'unit': row['Unit'],
                         'categories': categories,
                         'database': "biosphere3",
                         'type': "biosphere"
                        }) 

            up_dict.update({'exchanges': exchanges})
            inventories.append(up_dict)

        # Create inventory for global activity:
        activity_dic = {
                        'name': activity_name,
                        'reference product': activity_reference_product,
                        'location': activity_location,
                        "production amount": 1,
                        'unit': "unit",
                        "database": self.db_name,
                        "code": get_dataset_code(),
                        "comment": activity_comment
                        }
        activity_exchanges = []

        # Add production flow to exchanges
        activity_exchanges.append(get_production_flow_exchange(activity_dic)) 

        # Add unit processes to exchanges
        for up in inventories:
            activity_exchanges.append(
                                {
                                 'name': up["name"],
                                 "product": up["reference product"],
                                 "amount": 1,
                                 "unit": up["unit"],
                                 'database': up["database"],
                                 'location': up["location"],
                                 'type': "technosphere"
                                }
                )
            
        activity_dic.update({'exchanges': activity_exchanges})
        inventories.append(activity_dic)

        print("Linking datasets within the database and to ecoinvent and biosphere databases")

        link_exchanges_by_code(inventories, self.ecoinvent_db, self.biosphere_db)

        print("Done!")

        return inventories


    def create_lci_database(self):
        """
        Write the inventort database to Brightway and export the inventories to
        Excel file
        """
        inventories = self.format_inventories_for_bw()
        
        # check that export direct exists
        # otherwise we create it
        self.export_dir.mkdir(parents=True, exist_ok=True)

        filepath = self.export_dir / f"{self.db_name}_{datetime.datetime.today().strftime('%d-%m-%Y')}.xlsx"

        print("Writing LCI database to Brightway2 and exporting inventories in Excel file...")
        export_path = write_db_to_bw(inventories, self.db_name)

        # Copy the Excel file to the current location
        shutil.copy(export_path, filepath)

        return f"Database created and inventories exported to: {filepath}"