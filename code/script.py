import numpy as np
import pandas as pd
import pandera as pa
from pandera.errors import SchemaErrors,SchemaError


def do_validation_users(data):
    # schema = pa.DataFrameSchema({
    #     "user": pa.Column(int, nullable=False, required=True),
    #     "gender": pa.Column(pa.String, checks=pa.Check.isin(["M", "F"]), nullable=False),
    # },
    #     strict='filter', coerce='True')
    # try:
    #     schema.validate(data)
    # except SchemaError as err:
    #     # dataframe rows list of schema errors
    #     print(pd.DataFrame(err.data))
    data = data[data['vaccine'].isin(["M", "F"])]
    return pd.DataFrame(data)


def do_validation_vaccine(data):
    # # Defining the schema
    # schema = pa.DataFrameSchema({
    #     "user": pa.Column(int, nullable=False, required=True),
    #     "vaccine": pa.Column(pa.String, checks=pa.Check.isin(["A", "B", "C"]), nullable=False),
    #     "date": pa.Column(pa.DateTime, checks=pa.Check.in_range('2020-02-01', '2021-11-30'), nullable=False),
    # },
    #     strict='filter', coerce='True')
    # # apply validations
    # try:
    #     valid_df=schema.validate(data)
    # except SchemaError as err:
    #     # dataframe rows list of schema errors
    #     # print(pd.DataFrame(err.data))
    #     err=pd.DataFrame(err.data)
    valid_df = data[data['vaccine'].isin(["A", "B", "C"])]
    return pd.DataFrame(valid_df)


def process_your_file(file, user_df):
    vaccine_status_df = pd.read_csv(file, sep='\t')
    # validation on the data
    do_validation_vaccine(vaccine_status_df)
    # merge columns of user and their vaccine using user id
    res= pd.merge(user_df,vaccine_status_df,on="user",how="inner")
    res = res.groupby(["city","state","vaccine","gender"])["user"].count().reset_index(name="unique_vaccinated_people")
    return res


def covid_vaccine(vaccination_status_files, user_meta_file, output_file):
    """This function takes  vaccination_status_files and user_meta_file paths
    using this all the covid vaccination numbers needs to be stored in the given output file as TSV
    Args:
        vaccination_status_files: A List containing file path to the TSV vaccination_status_file.
        user_meta_file: A file path to TSV file containing User information.
        output_file: File path where output TSV results are should be stored, 
    Returns:
      None (doesnt return anything)
    """
    user_df = pd.read_csv(user_meta_file, sep='\t')
    i = 0
    for file in vaccination_status_files:
        res = process_your_file(file, user_df)
        print(res)
        if i == 0:
            res.to_csv(output_file, sep='\t',index=False)
        else:
            res.to_csv(output_file, mode='a', sep='\t',index=False)
        i += 1
