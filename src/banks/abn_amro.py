import re
import pandas as pd
from src.banks.base_adapter import BankAdapter


def _extract_merchant(desc: str) -> str:
    """Extract merchant name from ABN AMRO description field."""
    # Structured format: /NAME/merchant/
    m = re.search(r'/NAME/([^/]+)', desc)
    if m:
        return m.group(1).strip()
    # Plain format: Naam: merchant   (fields separated by 2+ spaces)
    m = re.search(r'Naam:\s*(.+?)(?:\s{2,}|$)', desc)
    if m:
        return m.group(1).strip()
    return ''


def _extract_description(desc: str) -> str:
    """Extract human-readable remittance info from ABN AMRO description field."""
    # Structured format: /REMI/info/
    m = re.search(r'/REMI/([^/]+)', desc)
    if m:
        return m.group(1).strip()
    # Plain format: Omschrijving: info   (fields separated by 2+ spaces)
    m = re.search(r'Omschrijving:\s*(.+?)(?:\s{2,}|$)', desc)
    if m:
        return m.group(1).strip()
    return desc.strip()


class AbnAmroAdapter(BankAdapter):
    def parse(self, filepath: str) -> pd.DataFrame:
        df = pd.read_excel(filepath, dtype=str, engine='xlrd')

        df['date'] = pd.to_datetime(df['transactiondate'], format='%Y%m%d').dt.date.astype(str)
        df['currency'] = df['mutationcode'].str.strip()

        df['amount'] = pd.to_numeric(df['amount'].str.replace(',', '.'), errors='coerce')

        df['merchant'] = df['description'].apply(_extract_merchant)
        df['description'] = df['description'].apply(_extract_description)
        df['source_account'] = 'ABN_AMRO'

        return df[['date', 'amount', 'merchant', 'description', 'currency', 'source_account']]
