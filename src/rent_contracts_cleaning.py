import os
import numpy as np 
import pandas as pd
from datetime import datetime, timedelta

class RentContractsDataCleaner:
    def __init__(self, rent_contracts_filename):
        # Define root path and paths to raw data files
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        raw_data_dir = os.path.join(self.project_root, "data", "raw")
        self.processed_data_dir = os.path.join(self.project_root, "data", "processed")
        
        # Ensure processed data directory exists
        os.makedirs(self.processed_data_dir, exist_ok=True)
        
        # Define file paths
        rent_contracts_path = os.path.join(raw_data_dir, rent_contracts_filename)
        
        # Load dataset
        self.rent_contracts = pd.read_csv(rent_contracts_path)
        
    def filtering_rent_contracts(self):
        # Converting dates to datetime
        self.rent_contracts["contract_start_date"] = pd.to_datetime(self.rent_contracts["contract_start_date"], format="%d-%m-%Y", errors="coerce")
        self.rent_contracts["contract_end_date"] = pd.to_datetime(self.rent_contracts["contract_end_date"], format="%d-%m-%Y", errors="coerce")
        
        # Filter rent contracts for the last three years
        current_year = datetime.now().year
        
        # Define the target years dynamically for the last three years
        target_years = [current_year - 3, current_year - 2, current_year - 1, current_year]
        self.rent_contracts = self.rent_contracts[self.rent_contracts["contract_start_date"].dt.year.isin(target_years)]
                
    def removing_arabic_cols(self):
        # Drop Arabic columns
        rent_contracts_arabic_columns = [col for col in self.rent_contracts.columns if col.endswith("_ar")]
        self.rent_contracts = self.rent_contracts.drop(columns=rent_contracts_arabic_columns)
        
    def filtering_residential(self):
        # Filter on "Residential" property usage
        self.rent_contracts = self.rent_contracts[self.rent_contracts["property_usage_en"] == "Residential"]
        
        # Dropping property usage column
        self.rent_contracts = self.rent_contracts.drop(columns=["property_usage_en"])
        
    def filtering_prop_line(self):
        # Filtering rent contracts to include only a single property per contract
        self.rent_contracts = self.rent_contracts[self.rent_contracts['no_of_prop'] == 1]
        
        # Filtering rent contracts to include only the first line number per contract
        self.rent_contracts = self.rent_contracts[self.rent_contracts['line_number'] == 1]
        
        # Removing no_of_prop & line_number columns from the dataset
        self.rent_contracts = self.rent_contracts.drop(columns=["no_of_prop", "line_number"])
        
    def filtering_residential_properties(self):
        # Filter the rent contracts data to include only residential property types
        self.rent_contracts = self.rent_contracts[self.rent_contracts['ejari_bus_property_type_en'].isin(['Unit', 'Villa'])]
        
    def filtering_property_types(self):
        # Define the property types we want to keep
        selected_property_types = ["Flat", "Villa", "Studio", "Complex Villas", "Arabian House", "Penthouse"]
        
        # Filter the DataFrame to keep only the rows with the specified property types
        self.rent_contracts = self.rent_contracts[self.rent_contracts["ejari_property_type_en"].isin(selected_property_types)]
        
    def filtering_non_residential_properties(self):
        # Filtering out non-residential sub property types
        non_residential_sub_types = ['Office', 'Room in labor Camp', 'Hotel', 'Pharmacy', 'Boardroom', 'Shop', 'Commercial villa']
        
        # Filtering the DataFrame to keep only the rows with the specified property types
        self.rent_contracts = self.rent_contracts[
            ~self.rent_contracts["ejari_property_sub_type_en"].isin(non_residential_sub_types)]
        
    def updating_property_types(self):
        # Update both ejari_property_type_en to 'Flat' and ejari_property_type_id to 842 where necessary
        self.rent_contracts.loc[
            (self.rent_contracts['ejari_bus_property_type_en'] == 'Unit') &
            (self.rent_contracts['ejari_property_type_en'] == 'Villa'),
            ['ejari_property_type_en', 'ejari_property_type_id']
        ] = ['Flat', 842]
            
        # Update both ejari_property_type_en to 'Villa' and ejari_property_type_id to 841 where necessary
        self.rent_contracts.loc[
            (self.rent_contracts['ejari_bus_property_type_en'] == 'Villa') &
            (self.rent_contracts['ejari_property_type_en'] == 'Flat'),
            ['ejari_property_type_en', 'ejari_property_type_id']
        ] = ['Villa', 841]
        
        # Update records where ejari_property_type_en is 'Villa' and ejari_property_sub_type_en is 'Studio'
        self.rent_contracts.loc[
        (self.rent_contracts['ejari_property_type_en'] == 'Villa') & 
        (self.rent_contracts['ejari_property_sub_type_en'] == 'Studio'),
        ['ejari_bus_property_type_id', 'ejari_bus_property_type_en', 'ejari_property_type_id', 'ejari_property_type_en']
        ] = [2.0, 'Unit', 842, 'Flat']
        
        # Update properties classified as 'Villa' with 'Duplex' in the sub-type to 'Unit' and 'Flat' with appropriate IDs
        self.rent_contracts.loc[
            (self.rent_contracts['ejari_property_type_en'] == 'Villa') & 
            (self.rent_contracts['ejari_property_sub_type_en'] == 'Duplex'),
            ['ejari_bus_property_type_en', 'ejari_bus_property_type_id', 'ejari_property_type_en', 'ejari_property_type_id']
        ] = ['Unit', 2.0, 'Flat', 842.0]
        
        # Update 'Unit' entries labeled as 'Arabian House' to 'Villa' with appropriate IDs
        self.rent_contracts.loc[
            (self.rent_contracts['ejari_bus_property_type_en'] == 'Unit') & 
            (self.rent_contracts['ejari_property_type_en'] == 'Arabian House'),
            ['ejari_bus_property_type_en', 'ejari_bus_property_type_id']
        ] = ['Villa', 4.0]
        
        # Update 'Unit' entries labeled as 'Complex Villas' to 'Villa' with appropriate IDs
        self.rent_contracts.loc[
            (self.rent_contracts['ejari_bus_property_type_en'] == 'Unit') & 
            (self.rent_contracts['ejari_property_type_en'] == 'Complex Villas'),
            ['ejari_bus_property_type_en', 'ejari_bus_property_type_id']
        ] = ['Villa', 4.0]

        # ejari_property_type_en is 'Complex Villas', and ejari_property_sub_type_en is 'Duplex'
        self.rent_contracts.loc[
            (self.rent_contracts['ejari_bus_property_type_en'] == 'Villa') & 
            (self.rent_contracts['ejari_property_type_en'] == 'Complex Villas') & 
            (self.rent_contracts['ejari_property_sub_type_en'] == 'Duplex'), 
            ['ejari_bus_property_type_id', 'ejari_bus_property_type_en', 'ejari_property_type_id', 'ejari_property_type_en']
        ] = [2.0, 'Unit', 842, 'Flat']
        
        # Update values for records where ejari_property_type_en is 'Flat' and ejari_property_sub_type_en is 'Penthouse'
        self.rent_contracts.loc[
            (self.rent_contracts['ejari_property_type_en'] == 'Flat') & 
            (self.rent_contracts['ejari_property_sub_type_en'] == 'Penthouse'), 
            ['ejari_property_type_id', 'ejari_property_type_en', 'ejari_property_sub_type_id', 'ejari_property_sub_type_en']
        ] = [352361946, 'Penthouse', 621, 'Penthouse']
        
    def impute_property_sub_type(self):
    # Group the data by project name and property sub-type to get min and max areas
        project_subtype_stats = (
            self.rent_contracts
            .groupby(['project_name_en', 'ejari_property_sub_type_en'])['actual_area']
            .agg(['min', 'max'])
            .reset_index()
        )

        # Define a function to impute the property sub-type based on the project name and area
        def impute_property_subtype_general_by_project(row):
            project_name = row['project_name_en']
            subtype = row['ejari_property_sub_type_en']
            area = row['actual_area']
            
            if pd.isna(subtype):  # Only attempt to impute if subtype is missing
                matching_stats = project_subtype_stats[
                    (project_subtype_stats['project_name_en'] == project_name) & 
                    (project_subtype_stats['ejari_property_sub_type_en'].notna())
                ]
                for _, stats_row in matching_stats.iterrows():
                    min_area, max_area = stats_row['min'], stats_row['max']
                    if min_area <= area <= max_area:
                        return stats_row['ejari_property_sub_type_en']
            
            return subtype  # Return original subtype if no match is found

        # Apply the project-based imputation function to the dataframe
        self.rent_contracts['ejari_property_sub_type_en'] = self.rent_contracts.apply(
            impute_property_subtype_general_by_project, axis=1
        )

        # Specific imputation for SANDHURST HOUSE flats based on area
        sandhurst_flats = (
            (self.rent_contracts['ejari_property_type_en'] == 'Flat') & 
            (self.rent_contracts['project_name_en'] == 'SANDHURST HOUSE') & 
            (self.rent_contracts['ejari_property_sub_type_en'].isna())
        )
        self.rent_contracts.loc[sandhurst_flats & (self.rent_contracts['actual_area'].between(47, 48)), 
                                'ejari_property_sub_type_en'] = 'Studio'

        # Group the data by area name and property sub-type to get min and max areas
        area_subtype_stats = (
            self.rent_contracts
            .groupby(['area_name_en', 'ejari_property_sub_type_en'])['actual_area']
            .agg(['min', 'max'])
            .reset_index()
        )

    # Define a function to impute the property sub-type based on area
    def impute_property_subtype_general(row):
        area_name = row['area_name_en']
        subtype = row['ejari_property_sub_type_en']
        actual_area = row['actual_area']
        
        if pd.isna(subtype):  # Only attempt to impute if subtype is missing
            matching_stats = area_subtype_stats[
                (area_subtype_stats['area_name_en'] == area_name) & 
                (area_subtype_stats['ejari_property_sub_type_en'].notna()) &
                (area_subtype_stats['ejari_property_sub_type_en'].isin(['Studio', '1bed room+Hall', '2 bed rooms+hall']))
            ]
            for _, stats_row in matching_stats.iterrows():
                min_area, max_area = stats_row['min'], stats_row['max']
                if min_area <= actual_area <= max_area:
                    return stats_row['ejari_property_sub_type_en']
        
        return subtype  # Return original subtype if no match is found

        # Apply the area-based imputation function to the dataframe
        self.rent_contracts['ejari_property_sub_type_en'] = self.rent_contracts.apply(
            impute_property_subtype_general, axis=1
        )
        
        # Replace "2 bed rooms+hall+Maids Room" with "2 bed rooms+hall"
        self.rent_contracts['ejari_property_sub_type_en'] = self.rent_contracts['ejari_property_sub_type_en'].replace(
            '2 bed rooms+hall+Maids Room', '2 bed rooms+hall'
        )
    
        # Creating a map of the numbers to fix the "ejari_property_sub_type_id" column
        sub_type_id_map = self.rent_contracts.groupby('ejari_property_sub_type_en')['ejari_property_sub_type_id'].first().to_dict()
        
        # Apply the mapping to the "ejari_property_sub_type_id" column
        self.rent_contracts['ejari_property_sub_type_id'] = self.rent_contracts['ejari_property_sub_type_en'].map(sub_type_id_map)
        
        # Create a boolean mask to identify the rows to delete
        mask = (self.rent_contracts['actual_area'] == 0) & \
            (self.rent_contracts['ejari_property_sub_type_en'] == '15 bed room+hall')

        # Use the mask to filter out the rows and create a new DataFrame
        self.rent_contracts = self.rent_contracts[~mask]
    
        # Define area limits for each property type
        villa_limits = (57, 15200)
        unit_limits = (9, 3201)

        # Filter observations based on ejari_bus_property_type_en and area limits
        self.rent_contracts = self.rent_contracts[
            ((self.rent_contracts['ejari_bus_property_type_en'] == 'Villa') & 
            (self.rent_contracts['actual_area'].between(*villa_limits))) |
            ((self.rent_contracts['ejari_bus_property_type_en'] == 'Unit') & 
            (self.rent_contracts['actual_area'].between(*unit_limits)))
        ].copy()
        
        # Standardize the 'ejari_property_sub_type_en' column by stripping whitespace and converting to lowercase
        self.rent_contracts['ejari_property_sub_type_en'] = self.rent_contracts['ejari_property_sub_type_en'].str.strip().str.lower()
    
        # Remove properties with 11-bedroom and 15-bedroom configurations
        self.rent_contracts = self.rent_contracts[
            ~self.rent_contracts['ejari_property_sub_type_en'].isin(['11 bed rooms+hall', '15 bed room+hall', 'room'])
        ]
        
        # Consolidate 8, 9, and 10-bedroom properties into a single category '8-10 Bed + Hall'
        self.rent_contracts['ejari_property_sub_type_en'] = np.where(
            self.rent_contracts['ejari_property_sub_type_en'].isin(['8 bed rooms+hall', '9 bed rooms+hall', '10 bed rooms+hall']),
            '8-10 Bed + Hall',
            self.rent_contracts['ejari_property_sub_type_en']
        )
    
        # Standardize category names for improved clarity
        name_replacements = {
            '1bed room+Hall': '1 Bed + Hall',
            '2 bed rooms+hall': '2 Beds + Hall',
            '3 bed rooms+hall': '3 Beds + Hall',
            '4 bed rooms+hall': '4 Beds + Hall',
            '5 bed rooms+hall': '5 Beds + Hall',
            '6 bed rooms+hall': '6 Beds + Hall',
            '7 bed rooms+hall': '7 Beds + Hall',
            'Studio': 'Studio',
            'Duplex': 'Duplex',
            'Penthouse': 'Penthouse'
        }
    
        # Apply the name replacements to the 'ejari_property_sub_type_en' column
        self.rent_contracts['ejari_property_sub_type_en'] = self.rent_contracts['ejari_property_sub_type_en'].replace(name_replacements)
        
        # Dropping tenant type columns from the dataset
        self.rent_contracts = self.rent_contracts.drop(columns=['tenant_type_id', 'tenant_type_en'])
    
    def amount_outliers_removal(self):
        # Filter out rows with zero values in contract_amount or annual_amount
        self.rent_contracts = self.rent_contracts[
            (self.rent_contracts['contract_amount'] > 0) &
            (self.rent_contracts['annual_amount'] > 0)
            ]
        
        # Calculate the 1st and 99th percentiles for both contract and annual amounts
        lower_contract, upper_contract = self.rent_contracts['contract_amount'].quantile([0.01, 0.99])
        lower_annual, upper_annual = self.rent_contracts['annual_amount'].quantile([0.01, 0.99])
        
        # Filter the dataset to retain only values within the 1st and 99th percentiles for both columns
        self.rent_contracts = self.rent_contracts[
            (self.rent_contracts['contract_amount'] >= lower_contract) & 
            (self.rent_contracts['contract_amount'] <= upper_contract) &
            (self.rent_contracts['annual_amount'] >= lower_annual) &
            (self.rent_contracts['annual_amount'] <= upper_annual)
        ]
    
    def removing_amenities_columns(self):
        # Selecting columns to drop
        amenities_columns = [col for col in self.rent_contracts.columns if 'nearest' in col]
        
        # Drop the 'nearest_amenities' column due to data inaccuracy
        self.rent_contracts = self.rent_contracts.drop(columns=amenities_columns)
        
    def creating_two_datasets(self):
        # Creating a dataset with non null project_name_en
        master_project_rent_contracts = self.rent_contracts[
            self.rent_contracts['project_number'].notnull()
        ].copy()
        
        # Saving the datasets to the processed data directory
        self.rent_contracts.to_csv(os.path.join(self.processed_data_dir, 'rent_contracts_3y.csv'), index=False)
        master_project_rent_contracts.to_csv(os.path.join(self.processed_data_dir, 'master_project_rent_contracts_3y.csv'), index=False)
        
    def run_cleaning(self):
        self.filtering_rent_contracts()
        self.removing_arabic_cols()
        self.filtering_residential()
        self.filtering_prop_line()
        self.filtering_residential_properties()
        self.filtering_property_types()
        self.filtering_non_residential_properties()
        self.updating_property_types()
        self.impute_property_sub_type()
        self.amount_outliers_removal()
        self.removing_amenities_columns()
        self.creating_two_datasets()
        
# Usage
cleaner = RentContractsDataCleaner('rent_contracts.csv')
cleaner.run_cleaning()