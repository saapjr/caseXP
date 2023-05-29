import pandas as pd

data = pd.read_csv('data/data.csv', sep=";", encoding="utf-8")

_asset_classes = ["CDB", "CRI", "CRA", "COE"]


def check_for_class(_class: str, asset_classes: list):
    return bool(_class in asset_classes)


def create_sub_datasets(df: pd.DataFrame, column_of_asset_classes: str):
    df = df.copy()

    df_CDB = df[df[column_of_asset_classes] == "CDB"]
    df_CRI = df[df[column_of_asset_classes] == "CRI"]
    df_CRA = df[df[column_of_asset_classes] == "CRA"]
    df_COE = df[df[column_of_asset_classes] == "COE"]

    df["NonValidClass"] = df[column_of_asset_classes].apply(check_for_class, args=(_asset_classes,))
    df_others = df[df["NonValidClass"] == False]
    df_others = df_others.drop(columns=["NonValidClass"])

    return zip(["CDB", "CRI", "CRA", "COE", "Others"], [df_CDB, df_CRI, df_CRA, df_COE, df_others])


def check_for_unity_prices(price):
    return bool(price in [0.1, 1, 100, 1000])


def check_prices(df: pd.DataFrame, column_of_prices: str):
    """Cria coluna CheckUnitPrices que verifica se os preços são 0.1, 1, 100 ou 1000"""

    df = df.copy()

    df["CheckUnitPrices"] = df[column_of_prices].apply(check_for_unity_prices)

    return df


def check_asset_class(df: pd.DataFrame, column_of_asset_class: str, asset_key: str):
    """Cria coluna CheckAssetClass que verifica se a classe do ativo é a mesma que a passada como parâmetro"""

    df = df.copy()

    df["CheckAssetClass"] = df[column_of_asset_class] == asset_key

    return df


def check_starts_with_asset_class(df: pd.DataFrame, column_of_negotiation_code: str, asset_key: str):
    df = df.copy()

    df["CheckStartsWithAssetClass"] = df[column_of_negotiation_code].str[:3]
    df["CheckStartsWithAssetClass"] = df["CheckStartsWithAssetClass"] == asset_key

    return df


def check_starts_with_emission_year(df: pd.DataFrame, column_of_negotiation_code: str):

    df = df.copy()

    df["CheckStartsWithEmissionYear"] = df[column_of_negotiation_code].str[:2]
    df["IssueYear"] = df["IssueDate"].str[-2:]
    df["CheckStartsWithEmissionYear"] = df["CheckStartsWithEmissionYear"] == df["IssueYear"]

    df = df.drop(columns=["IssueYear"])

    return df


def check_year_digits(df: pd.DataFrame, column_of_negotiation_code: str):
    df = df.copy()

    df["CheckYearDigits"] = df[column_of_negotiation_code].str[4:6]
    df["CheckYearDigits"] = df["CheckYearDigits"].str.isnumeric()

    return df


def check_length_of_negotiation_code(df: pd.DataFrame, column_of_negotiation_code: str):
    """Cria coluna CheckLengthOfNegotiationCode que verifica se o código de negociação tem 11 caracteres"""

    df = df.copy()

    df["NegotiationCodeLength"] = df[column_of_negotiation_code].apply(len)

    df["CheckLengthOfNegotiationCode"] = df["NegotiationCodeLength"] == 11

    df = df.drop(columns=["NegotiationCodeLength"])

    return df


def check_for_issue_dates(df: pd.DataFrame, column_of_issue_date: str, column_of_maturity_date: str):
    """Cria coluna CheckBoughtOnIssueDates que verifica se as datas de compra e vencimento são iguais """

    df = df.copy()

    df["CheckBoughtOnIssueDates"] = df[column_of_issue_date] == df[column_of_maturity_date]

    return df


def check_for_unit_and_date(df: pd.DataFrame, column_of_unit_price: str, column_of_issue_date: str):
    """Cria coluna CheckUnitAndDate que verifica se preço é unitário e foi comprado na emissão"""

    df = df.copy()

    df["CheckUnitAndDate"] = ((df[column_of_unit_price] & df[column_of_issue_date]) |
                              (~df[column_of_unit_price] & ~df[column_of_issue_date]))

    return df


def check_for_emission_date(df: pd.DataFrame, column_of_negotiation_code: str, column_of_issue_date: str):

    df = df.copy()

    df["EmissionDate"] = df[column_of_negotiation_code].str[4:6]
    df["IssueDate_"] = df[column_of_issue_date].str[-2:]

    df["CheckIssueDateNegotiationCode"] = df["IssueDate_"] == df["EmissionDate"]

    df = df.drop(columns=["EmissionDate", "IssueDate_"])

    return df


def is_alpha_numeric(df: pd.DataFrame, column_of_negotiation_code: str, begin: int, end: int):
    df = df.copy()

    df["CheckAlphaNumeric"] = df[column_of_negotiation_code].str[begin:end]
    df["CheckAlphaNumeric"] = df["CheckAlphaNumeric"].str.isalnum()

    return df


def check_month_of_emission(df: pd.DataFrame, column_of_negotiation_code: str, column_of_issue_date: str):

    df = df.copy()

    df["EmissionMonth"] = df[column_of_negotiation_code].str[2:4]
    df["IssueMonth"] = df[column_of_issue_date].str[3:5]

    df["CheckMonthOfEmission"] = df["EmissionMonth"] == df["IssueMonth"]

    df = df.drop(columns=["EmissionMonth", "IssueMonth"])

    return df


def check_negotiation_code(_class: str, df: pd.DataFrame, column_of_negotiation_code: str, column_of_issue_date: str):
    """Cria coluna CheckNegotiationCode que verifica se o código de negociação está de acordo com a classe do ativo"""

    df = df.copy()

    if _class in ["CDB", "CRA"]:

        df = check_starts_with_asset_class(df, column_of_negotiation_code, _class)

        df = check_year_digits(df, column_of_negotiation_code)

        df = check_for_emission_date(df, column_of_negotiation_code, column_of_issue_date)

        df = is_alpha_numeric(df, column_of_negotiation_code, 7, 12)

        df["CheckNegotiationCode"] = (df["CheckStartsWithAssetClass"] &
                                      df["CheckYearDigits"] &
                                      df["CheckIssueDateNegotiationCode"] &
                                      df["CheckAlphaNumeric"])

        df = df.drop(columns=["CheckStartsWithAssetClass", "CheckYearDigits",
                              "CheckIssueDateNegotiationCode", "CheckAlphaNumeric"])

    elif _class in ["CRI"]:
        df = check_starts_with_emission_year(df, column_of_negotiation_code)
        df = check_month_of_emission(df, column_of_negotiation_code, column_of_issue_date)
        df = is_alpha_numeric(df, column_of_negotiation_code, 4, 12)

        df["CheckNegotiationCode"] = (df["CheckStartsWithEmissionYear"] &
                                      df["CheckAlphaNumeric"] &
                                      df["CheckMonthOfEmission"])

        df = df.drop(columns=["CheckStartsWithEmissionYear", "CheckMonthOfEmission", "CheckAlphaNumeric"])

    elif _class in ["COE"]:

        df = check_starts_with_emission_year(df, column_of_negotiation_code)
        df = check_month_of_emission(df, column_of_negotiation_code, column_of_issue_date)
        df = is_alpha_numeric(df, column_of_negotiation_code, 4, 12)

        df["CheckNegotiationCode"] = (df["CheckStartsWithEmissionYear"] &
                                      df["CheckMonthOfEmission"] &
                                      df["CheckAlphaNumeric"])

        df = df.drop(columns=["CheckStartsWithEmissionYear", "CheckMonthOfEmission", "CheckAlphaNumeric"])

    return df


def build_flag(df: pd.DataFrame, columns: list):
    """Cria coluna Flag que verifica se as colunas de validação estão todas como True"""

    df = df.copy()

    df["Flag"] = df[columns].all(axis=1)

    return df


result_df = pd.DataFrame()

for key, value in create_sub_datasets(data, "AssetClass"):

    transactions = check_prices(value, "UnitPrice")

    transactions = check_for_issue_dates(transactions, "IssueDate", "BuyDate")

    transactions = check_for_unit_and_date(transactions, "CheckUnitPrices", "CheckBoughtOnIssueDates")

    transactions = check_asset_class(transactions, "AssetClass", key)

    transactions = check_length_of_negotiation_code(transactions, "NegotiationCode")

    transactions = check_negotiation_code(key, transactions, "NegotiationCode", "IssueDate")

    transactions["Subset"] = key

    if result_df.empty:
        result_df = transactions
    else:
        result_df = pd.concat([result_df, transactions])


result_df = build_flag(result_df, ["CheckUnitAndDate", "CheckAssetClass", "CheckLengthOfNegotiationCode"])

result_df.to_csv("data/result.csv")
