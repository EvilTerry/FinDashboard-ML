import pandas as pd
from src.banks.base_adapter import BankAdapter


class SwedbankAdapter(BankAdapter):
    def parse(self, filepath: str) -> pd.DataFrame:
        df = pd.read_csv(filepath, decimal='.', thousands=',')

        # Select only the columns we need — avoids fragile positional drops
        df = df[['Date', 'Beneficiary', 'Details', 'Amount', 'Currency', 'D/K']]

        df = df.rename(columns={
            'Date': 'date',
            'Beneficiary': 'merchant',
            'Details': 'description',
            'Amount': 'amount',
            'Currency': 'currency',
        })

        # Amounts are always positive — D/K tells us direction (D=debit, K=credit)
        df['amount'] = df['amount'].where(df['D/K'] != 'D', -df['amount'])
        df = df.drop(columns='D/K')

        df['source_account'] = 'SWEDBANK'

        exact_noise = ['Opening balance', 'Closing balance', 'Turnover', 'MP banko mokestis', 'TMP banko mokestis']
        pattern_noise = ['Minimalus kasdienių paslaugų mokestis', 'Kortelės mėnesinis administravimo mokestis']

        df = df[~df['description'].isin(exact_noise)]
        df = df[~df['description'].str.contains('|'.join(pattern_noise), na=False)]

        return df
