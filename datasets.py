import pandas as pd 

# class NewYorkFed():

class HousingDatasets():
    
    def __init__(self):
        self.path_folder = 'data/'

    def load_recessions(self):
        """List of US Recessions gathered from Wikipedia"""
        path = self.path_folder + 'List_of_recessions_in_the_United_States.xlsx'
        recessions = pd.read_excel(path)
        recessions['Start'] = pd.to_datetime(recessions['Start'])
        recessions['End'] = pd.to_datetime(recessions['End'])
        return recessions
    

    ######################################
    # stlouisfed data functions
    ######################################

    def load_treasury_spread(self):
        return self.load_stlouisfed_data("T10Y3M")

    def load_active_listings(self):
        return self.load_stlouisfed_data("ACTLISCOUUS")
    
    def load_new_listings(self):
        return self.load_stlouisfed_data("NEWLISCOUUS")

    def load_new_house_supply(self):
        return self.load_stlouisfed_data("MSACSR")
    
    def load_new_house_started(self):
        return self.load_stlouisfed_data("HOUST")
    
    def load_unemployment_rate(self):
        return self.load_stlouisfed_data("UNRATE")

    def load_median_days_on_market(self):
        return self.load_stlouisfed_data("MEDDAYONMARUS")
    
    def load_median_house_price(self):
        return self.load_stlouisfed_data("MSPUS")
    
    def load_median_household_income(self):
        return self.load_stlouisfed_data("MEHOINUSA672N")
    
    def load_case_shiller(self):
        return self.load_stlouisfed_data("CSUSHPINSA")

    def load_housing_affordability(self):
        return self.load_stlouisfed_data("FIXHAI")

    def load_avg_mortgage_rates(self):
        return self.load_stlouisfed_data("MORTGAGE30US")

    def load_stlouisfed_data(self, file_name):
        ''' Generic loader for St Louis Fed data since they all share similar attributes'''
        df = pd.read_csv(f'{self.path_folder}{file_name}.csv')
        df.rename(columns={f"{file_name}":'value', "DATE":'date'}, inplace=True)
        df['date'] = df['date'].astype('datetime64[us]')
        return df
    
    ######################################
    # newyorkfed data functions
    ######################################

    def load_loan_report(self):
        '''
        Total Debt Balance and Its Composition
        Value is in Trillions of $
        returns df, title, value_label
        '''
        return self.load_newyorkfed_data(sheet_name='Page 3 Data', header=3) 

    def load_loan_report_by_num_accounts(self):
        '''
        Number of Accounts by Loan Type
        Value is in Millions of accounts
        '''
        return self.load_newyorkfed_data(sheet_name='Page 4 Data', header=3)

    def load_mortgage_credit_scores(self):
        return self.load_newyorkfed_data(sheet_name='Page 6 Data', header=3)

    def load_auto_credit_scores(self):
        return self.load_newyorkfed_data(sheet_name='Page 8 Data', header=3)
    
    def load_delinquent_loans(self):
        return self.load_newyorkfed_data(sheet_name='Page 13 Data', header=4)

    def load_foreclosures(self):
        return self.load_newyorkfed_data(sheet_name='Page 17 Data', header=3)
    
    def convert_to_datetime(self, date):
        ''' Convert mixed date formats to datetime
            For example: '2003-03-01 00:00:00' and '11:Q2' formating
        '''
        if ' ' not in date:
            year, quarter = date.split(':')
            year = '20' + year  # Assuming the years are in the 2000s
            quarter_month = {'Q1': '03-01', 'Q2': '06-01', 'Q3': '09-01', 'Q4': '12-01'}
            return pd.to_datetime(f'{year}-{quarter_month[quarter]}')
        else:  # Handling the standard date format
            return pd.to_datetime(date)
    
    # newyorkfed
    def clean_date_column(self, df):
        df.rename(columns={'Unnamed: 0': 'Date'}, inplace=True)
        df['Date'] = df['Date'].astype(str)
        df['Date'] = df['Date'].apply(self.convert_to_datetime)
        df.rename(columns={"Date":'date'}, inplace=True)
        return df

    # newyorkfed
    def drop_unnamed_columns(self, df):
        """Drops any columns from the DataFrame that contain 'Unnamed' in the column name."""
        return df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    # newyorkfed
    def prep_newyorkfed_data(self, df):
        df = self.clean_date_column(df)
        return self.drop_unnamed_columns(df)
    
    def load_newyorkfed_data(self, sheet_name, header):
        path = self.path_folder + 'newyorkfed_household_debit_and_credit_report.xlsx'
        # df = pd.read_excel(path, sheet_name=sheet_name, header=0)
        header_df = pd.read_excel(
            path, 
            sheet_name=sheet_name, 
            header=0,
            nrows=2
        )
        title = header_df.columns[0]
        value_label = header_df.iloc[0, 0]
        if '*' in title:
            title += f'({header_df.iloc[1, 0]})'
        df = pd.read_excel(path, sheet_name=sheet_name, header=header) 
        df = self.prep_newyorkfed_data(df)
        return df, title, value_label 