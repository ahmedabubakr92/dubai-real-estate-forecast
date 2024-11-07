import os
import numpy as np
import pandas as pd
from datetime import datetime

class TransactionsDataCleaner:
    def __init__(self, transactions_filename, projects_filename, developers_filename):
        # Define root path and paths to raw data files
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        raw_data_dir = os.path.join(self.project_root, 'data', 'raw')
        self.processed_data_dir = os.path.join(self.project_root, 'data', 'processed')
        
        # Ensure processed data directory exists
        os.makedirs(self.processed_data_dir, exist_ok=True)

        # Define file paths
        transactions_path = os.path.join(raw_data_dir, transactions_filename)
        projects_path = os.path.join(raw_data_dir, projects_filename)
        developers_path = os.path.join(raw_data_dir, developers_filename)
        
        # Load datasets
        self.transactions = pd.read_csv(transactions_path)
        self.projects = pd.read_csv(projects_path)
        self.developers = pd.read_csv(developers_path)

    def filtering_transactions(self):
        # Filter transactions for last 3 years
        self.transactions['instance_date'] = pd.to_datetime(self.transactions['instance_date'], format='%d-%m-%Y', errors='coerce')
        self.transactions = self.transactions[self.transactions['instance_date'].dt.year >= datetime.now().year - 3]
        
        # Drop Arabic columns
        transactions_arabic_columns = [col for col in self.transactions.columns if col.endswith("_ar")]
        self.transactions.drop(columns=transactions_arabic_columns, inplace=True)
        
        # Drop nearest amenities and parties columns
        amenities_columns = [col for col in self.transactions.columns if col.startswith('nearest')]
        self.transactions.drop(columns=amenities_columns, inplace=True)
        parties_columns = [col for col in self.transactions.columns if "parties" in col]
        self.transactions.drop(columns=parties_columns, inplace=True)

        # Reordering columns in transactions
        cols_to_keep = ['instance_date', 'transaction_id', 'trans_group_id', 'trans_group_en', 'procedure_id', 'procedure_name_en',
                        'reg_type_id', 'reg_type_en', 'property_usage_en', 'property_type_id', 'property_type_en', 
                        'property_sub_type_id', 'property_sub_type_en', 'area_id', 'area_name_en', 'master_project_en',
                        'project_number', 'project_name_en', 'building_name_en', 'rooms_en', 'has_parking', 'procedure_area', 
                        'meter_sale_price', 'actual_worth']
        self.transactions = self.transactions[cols_to_keep]
        
        # Save filtered dataset with full information for last 3 years
        self.transactions.to_csv(os.path.join(self.processed_data_dir, 'transactions_3y.csv'), index=False)
        
        # Filter on "Sales" transactions
        self.transactions = self.transactions[self.transactions['trans_group_en'] == 'Sales']
        self.transactions.drop(columns=['trans_group_id', 'trans_group_en'], inplace=True)
        
        # Filter on "Sell" and "Sell - Pre registration" procedures
        self.transactions = self.transactions[
            self.transactions['procedure_name_en'].isin(['Sell', 'Sell - Pre registration'])]
        self.transactions.drop(columns=['procedure_id', 'procedure_name_en'], inplace=True)
        
        # Filter on "Residential" property usage
        self.transactions = self.transactions[self.transactions['property_usage_en'] == 'Residential']
        self.transactions.drop(columns=['property_usage_en'], inplace=True)
        
        # Filter on "Villa" & "Unit" property types
        self.transactions = self.transactions[self.transactions['property_type_en'].isin(['Villa', 'Unit'])]

        # Reclassify "Stacked Townhouses" as "Villa" based on project name
        self.transactions.loc[
            self.transactions['project_name_en'].str.contains("townhouse", case=False, na=False),
            ['property_type_id', 'property_type_en']
        ] = [4, 'Villa']

        # Adjust "Flat" entries under "Villa" to correct type
        self.transactions.loc[
            (self.transactions['property_type_en'] == 'Villa') & (self.transactions['property_sub_type_en'] == 'Flat'),
            ['property_sub_type_id', 'property_sub_type_en']
        ] = [4, 'Villa']
        
        # Drop unnecessary property sub type columns
        self.transactions.drop(columns=['property_sub_type_id', 'property_sub_type_en'], inplace=True)

    def area_outlier_removal(self):
        # Define reasonable limits for each property type
        unit_area_limits = (21, 2988)
        villa_area_limits = (47, 10062)

        # Filter based on property type and area limits
        def filter_procedure_area_by_type(row):
            property_type = row['property_type_en']
            area = row['procedure_area']
            
            if property_type == "Unit":
                return unit_area_limits[0] <= area <= unit_area_limits[1]
            elif property_type == "Villa":
                return villa_area_limits[0] <= area <= villa_area_limits[1]
            return False
        
        self.transactions = self.transactions[self.transactions.apply(filter_procedure_area_by_type, axis=1)]

    def drop_null_location_observations(self):
        # Drop observations where key location columns are all null
        self.transactions = self.transactions[~(
            self.transactions['rooms_en'].isnull() &
            self.transactions['building_name_en'].isnull() &
            self.transactions['project_number'].isnull() &
            self.transactions['master_project_en'].isnull()
        )]

    def map_project_names(self):
        # Map project names from transactions to projects dataset using project_number
        project_name_map = (self.transactions
                            .dropna(subset=['project_number'])
                            .drop_duplicates(subset=['project_number'])
                            .set_index('project_number')['project_name_en']
                            .to_dict())
        
        self.projects['project_name_en'] = self.projects['project_number'].map(project_name_map)

    def clean_projects(self):
        # Select and clean specific columns in the projects dataset
        columns_to_keep = [
            'project_number', 'project_name_en', 'developer_number', 'developer_name',
            'master_developer_number', 'master_developer_name', 'project_start_date',
            'project_end_date', 'project_status', 'percent_completed', 'completion_date',
            'area_id', 'area_name_en', 'master_project_en'
        ]
        self.projects = self.projects[columns_to_keep].copy()

    def clean_developers(self):
        # Select specific columns in developers dataset
        columns_to_keep = ['developer_number', 'developer_name_en']
        self.developers = self.developers[columns_to_keep].copy()

    def merge_projects_developers(self):
        # Merge cleaned projects and developers datasets
        self.projects_developers = self.projects.merge(self.developers, on='developer_number', how='left')

    def finalize_projects_developers(self):
        # Reorder columns and save the merged dataset
        self.projects_developers = self.projects_developers[[
            'project_number', 'project_name_en', 'developer_name_en', 'area_id', 'area_name_en',
            'master_project_en', 'project_start_date', 'project_end_date', 'project_status',
            'percent_completed', 'completion_date'
        ]]
        self.projects_developers.to_csv(os.path.join(self.processed_data_dir, 'projects_developers.csv'), index=False)

    def merge_transactions_projects(self):
        # Merge transactions with projects_developers
        self.transactions_merged = self.transactions.merge(self.projects_developers, on='project_number', how='left')
        self.transactions_merged.to_csv(os.path.join(self.processed_data_dir, 'transactions_merged_auto.csv'), index=False)

    def select_columns_from_merged(self):
        # Select specified columns from merged dataset and drop null project numbers
        self.transactions_merged_cleaned = self.transactions_merged[[
            'instance_date', 'transaction_id', 'reg_type_en', 'property_type_en', 'property_type_id',
            'area_id_x', 'area_name_en_x', 'project_number', 'project_name_en_x', 'rooms_en', 
            'has_parking', 'procedure_area', 'meter_sale_price', 'actual_worth', 'developer_name_en',
            'project_start_date', 'project_end_date', 'completion_date', 'project_status', 'percent_completed'
        ]].copy()
        self.transactions_merged_cleaned.dropna(subset=['project_number'], inplace=True)
        self.transactions_merged_cleaned.to_csv(os.path.join(self.processed_data_dir, 'transactions_merged_cleaned.csv'), index=False)

    def run_cleaning(self):
        self.filtering_transactions()
        self.area_outlier_removal()
        self.drop_null_location_observations()
        self.map_project_names()
        self.clean_projects()
        self.clean_developers()
        self.merge_projects_developers()
        self.finalize_projects_developers()
        self.merge_transactions_projects()
        self.select_columns_from_merged()

# Usage
cleaner = TransactionsDataCleaner('transactions.csv', 'projects.csv', 'developers.csv')
cleaner.run_cleaning()