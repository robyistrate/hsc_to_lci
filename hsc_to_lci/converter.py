from .utils import (
    import_ecoinvent_as_dict,
    import_biosphere_as_dict,
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
import brightway2 as bw
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

        self.simulation_file = self.metadata['input files']['simulation file']
        self.mapping_file = self.metadata['input files']['mapping file']

        self.bw_proj = self.metadata['brightway project']['project name']
        self.source_db = self.metadata['brightway project']['ecoinvent database']
        # Open Brightway project
        bw.projects.set_current(self.bw_proj)

        self.activity_description = self.metadata['activity description']
        
        self.ecoinvent_db = import_ecoinvent_as_dict(self.source_db)
        self.biosphere_db = import_biosphere_as_dict()
        self.ecoinvent_units = get_ecoinvent_units()
        self.simulation_lci_map = get_simulation_lci_map(self.mapping_file)

        # export directory is the current working
        # directory unless specified otherwise
        if export_dir:
            self.export_dir = Path(export_dir)
        else:
            self.export_dir =  Path.cwd()


    def get_simulation_results_data(self):
        """
        Extract input and output streams data.

        - Input streams are all considered to be input products from the technosphere
        - Output streams can be emissions to the environmental or waste streams (i.e., technosphere flows)

        :return: dataframe containing the input or output streams data for each unit process
        """

        print("Extract process simulation results data")

        technosphere_flows = [i for i in self.simulation_lci_map
                              if self.simulation_lci_map[i]["LCI flow type"] == "technosphere"]
        biosphere_flows = [i for i in self.simulation_lci_map
                           if self.simulation_lci_map[i]["LCI flow type"] == "biosphere"]        

        simulation_results_raw = pd.ExcelFile(self.simulation_file)

        simulation_results_processed = pd.DataFrame()

        for sheet in ['Input Streams', "Output Streams"]:
            df = simulation_results_raw.parse(sheet)

            # Format the imported df
            new_header = df.iloc[0]
            df.columns = new_header
            df = df[1:]
            df = df.drop(
                columns=["Use Exergy", "LCA Equivalent", "LCA Group", "Main Product"]
                )
            
            # Give header name to property columns
            df.columns.values[1] = "Stream Property Name"
            df.columns.values[4] = "Stream Property Amount"
            df.columns.values[6] = "Stream Property Unit"

            # For input streams, keep only the "Stream Name" row
            if sheet == "Input Streams":
                df = df.dropna(subset="Stream Name").dropna(axis=1, how='all')
                df = df.set_index('Unit Name').sort_index(axis=0)

            # For output streams, keep the Stream Properties
            elif sheet == "Output Streams":

                # Clean the properties data
                for col in ['Stream Name', "Unit Name", "Amount", "Unit"]:
                    df[col].fillna(method='ffill', inplace=True)

                df.dropna(subset=['Stream Property Name'], inplace=True)
                df = df[df['Stream Property Name'] != "Name"]
                df["Stream Property Amount"] = df["Stream Property Amount"].apply(lambda x: x.replace(',', '.') if isinstance(x, str) else x)

                for col in ["Amount", "Stream Property Amount"]:
                    df[col] = df[col].astype(float)
                
                df = df.dropna(axis=1, how='all')

                # Get output data distinguishing between technosphere/biosphere
                output_data = []
                for index, row in df.iterrows():

                    stream_name = df.loc[index]["Stream Name"]
                    stream_amount = float(df.loc[index]["Amount"])
                    stream_unit = df.loc[index]["Unit"]
                    
                    property_amount = df.loc[index]["Stream Property Amount"]
                    property_unit = df.loc[index]["Stream Property Unit"]

                    mass_flow = float(df[(df["Stream Name"] == stream_name) & (df["Stream Property Name"] == "Mass Flow")]["Stream Property Amount"].values[0])

                    # Output streams that are technosphere flows (i.e., solid waste and wastewater treatment):
                    if row["Stream Name"] in technosphere_flows:
                        total_solids_flow = float(df[(df["Stream Name"] == stream_name) & (df["Stream Property Name"] == "Total Solids Flow")]["Stream Property Amount"].values[0])
                        total_liquid_flow = float(df[(df["Stream Name"] == stream_name) & (df["Stream Property Name"] == "Total Liquid Flow")]["Stream Property Amount"].values[0])

                        if total_solids_flow > 0:
                            lci_amount = - total_solids_flow / mass_flow * stream_amount
                    
                        if total_liquid_flow > 0:
                            lci_amount = - total_liquid_flow / mass_flow * stream_amount
                    
                        output_stream_data = {
                            "Unit Name": row["Unit Name"],
                            "Stream Name": row["Stream Name"],
                            "Amount": lci_amount,
                            "Unit": stream_unit
                            }
                        if output_stream_data not in output_data:
                            output_data.append(output_stream_data)

                    else:
                        if row["Stream Property Name"] in biosphere_flows:
                            # Waste heat
                            if row["Stream Property Unit"] in ["kW", "kWh", "kilowatt hour", "MJ", "megajoule"]:
                                lci_amount = property_amount
                            # Emission of substances to air, water, or soil
                            else:
                                lci_amount = property_amount / mass_flow * stream_amount
                        else:
                            lci_amount = np.nan

                        if not np.isnan(lci_amount):
                            output_stream_data = {
                                "Unit Name": row["Unit Name"],
                                "Stream Name": row["Stream Property Name"],
                                "Amount": lci_amount,
                                "Unit": stream_unit
                                }

                            if output_stream_data not in output_data:
                                output_data.append(output_stream_data)

                df = pd.DataFrame(output_data)
                df = df.set_index('Unit Name').sort_index(axis=0)
                df = df[df['Amount'] != 0]

            df['Stream type'] = sheet # add stream type (input or output)

            simulation_results_processed = pd.concat([simulation_results_processed, df])
            simulation_results_processed = simulation_results_processed.sort_index()

        print("Apply strategies: Add technosphere/biosphere flow type")
        simulation_results_processed['LCI type'] = np.where(
            simulation_results_processed['Stream Name'].isin(technosphere_flows), 'technosphere',
            np.where(simulation_results_processed['Stream Name'].isin(biosphere_flows), 'biosphere', None)
            )
    
        print("Apply strategies: Change units to ecoinvent format")
        simulation_results_processed['Unit'] = simulation_results_processed['Unit'].apply(lambda x: self.ecoinvent_units.get(x, x))

        print("Apply strategies: Convert process simulation units to ecoinvent units")
        units_conversion(simulation_results_processed)

        return simulation_results_processed


    def format_inventories_for_bw(self):
        """
        Format inventories to Brightway2 format.
        :return: list of dictionaries; each dictionary represents a unit process inventory
        """

        print("Format inventories to Brightway2 format")

        activity_name = self.metadata['activity description']['name']
        activity_reference_product = self.metadata['activity description']['reference product']
        activity_location = self.metadata['activity description']['location']
        activity_db = self.metadata['activity description']['database']
        activity_comment = self.metadata['activity description']['comment']

        simulation_results_data = self.get_simulation_results_data()
        print(simulation_results_data[simulation_results_data["Stream Name"] == "Natural gas"])
        # List of unit processes
        unit_processes = list(set(simulation_results_data.index))
        
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
                       "database": activity_db,
                       "code": get_dataset_code(),
                       "comment": activity_comment
                       }
            
            # Add exchanges to the inventory
            exchanges = []

            # Add production flow to exchanges
            exchanges.append(get_production_flow_exchange(up_dict)) 

            for index, row in simulation_results_data.loc[[up]].iterrows():

                if row["LCI type"] == "technosphere":

                    exc_filter = {
                        'name': self.simulation_lci_map[row["Stream Name"]]["Name"],
                        'product': self.simulation_lci_map[row["Stream Name"]]["Reference product"],
                        'unit': row['Unit']
                    }

                    try:
                        exc_dataset = get_dataset_for_location(
                            activity_location,
                            exc_filter,
                            self.ecoinvent_db
                            )
                    except IndexError:
                        raise ValueError(f"No LCI dataset available for {exc_filter}")
                
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
                        "database": activity_db,
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
        new_db_name = self.metadata['activity description']['database']

        # check that export direct exists
        # otherwise we create it
        self.export_dir.mkdir(parents=True, exist_ok=True)

        filepath = self.export_dir / f"{new_db_name}_{datetime.datetime.today().strftime('%d-%m-%Y')}.xlsx"

        print("Writing LCI database to Brightway2 and exporting inventories in Excel file...")
        export_path = write_db_to_bw(inventories, new_db_name)

        # Copy the Excel file to the current location
        shutil.copy(export_path, filepath)

        return f"Database created and inventories exported to: {filepath}"