import pandas as pd
from src.banks.base_adapter import BankAdapter


class BunqAdapter(BankAdapter):
    def parse(self, filepath: str) -> pd.DataFrame:
        df = pd.read_csv(filepath, decimal=',', thousands='.')

        df = df.rename(columns={
            'Date': 'date',
            'Amount': 'amount',
            'Name': 'merchant',
            'Description': 'description',
        })

        df['currency'] = 'EUR'
        df['source_account'] = 'BUNQ'

        df = df[['date', 'amount', 'merchant', 'description', 'currency', 'source_account']]
        df = df[~df['description'].isin(['Opening balance', 'Closing balance', 'Turnover'])]

        return df
