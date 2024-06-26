import azure.functions as func
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient, ContentSettings
import os
import logging
from datetime import datetime
from sklearn.model_selection import train_test_split

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json() if req.get_body() else {}
        # Connection string to your Azure Storage account
        connection_string = os.environ['AzureWebJobsStorage']
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        source_container_client = blob_service_client.get_container_client("silver-level")
        destination_container_client = blob_service_client.get_container_client("gold-level")

        final_df = pd.DataFrame()  # Final dataframe to hold all the data

        # List directories (which are by year)
        years = ['1999-2000', '2001-2002', '2003-2004','2005-2006','2007-2008','2009-2010','2011-2012','2013-2014','2015-2016','2017-2020']  # List all years here
        patterns = ['BMX', 'DBQ', 'DEMO', 'OHQ', 'SLQ','SMQ', 'SMQFAM', 'SMQRTU', 'SMQMEC', 'WHQ','COT']
        
        for year in years:
            year_df = pd.DataFrame()  # DataFrame to hold year-specific data

            # List files in the year directory
            file_blobs = source_container_client.list_blobs(name_starts_with=f'{year}/')
            
            for blob in file_blobs:
                file_name = blob.name.split('/')[1].upper()
                # if the file has an underscore Remove underscores and any characters after it 
                if '_' in file_name:
                    clean_file_name = file_name.split('_')[0]
                else:
                    clean_file_name = file_name.split('.')[0]

                # Check if the file name contains any of the patterns
                if clean_file_name in patterns:
                    blob_client = source_container_client.get_blob_client(blob)
                    blob_data = blob_client.download_blob().readall()
                    data = pd.read_csv(BytesIO(blob_data))  
                    data.columns = data.columns.str.upper()  # Convert column names to uppercase

                    # Mapping of file names to processing functions
                    file_processing_functions = {
                        'BMX': process_bmx_file,
                        'DBQ': process_dbq_file,
                        'DEMO': process_demo_file,
                        'OHQ': process_ohq_file,
                        'SLQ': process_slq_file,
                        'SMQ': process_smq_file,
                        'SMQFAM': process_smqfam_file,
                        'SMQMEC': process_smqmec_file,
                        'WHQ': process_whq_file,
                        'SMQRTU': process_smqrtu_file,
                        'COT': process_cot_file
                    }

                    # Apply specific transformations based on the file pattern using the mapping
                    if clean_file_name in file_processing_functions:
                        data = file_processing_functions[clean_file_name](year, data)
                    else:
                        raise ValueError(f"File name {file_name} does not match any of the patterns.")
                    # Merge the data from this file to the year-specific DataFrame
                    if year_df.empty:
                        year_df = data
                    else:
                        year_df = pd.merge(year_df,data, on='SEQN', how='outer')
                    
                    year_df['Year'] = year  # Add a column to the year-specific DataFrame to indicate the year


            # Add the year-specific data to the final DataFrame using skle
            final_df = pd.concat([final_df, year_df], axis=0)

        # Split the dataframe into train and test sets
        train_df, test_df = train_test_split(final_df, test_size=0.3, random_state=42)

        if 'RIDRETH1' in train_df.columns:
            train_df = train_df.drop(columns=['RIDRETH1'])

        # Upload the final DataFrame to the 'gold-level' container
        # Convert the train and test DataFrames to CSV
        train_csv_data = train_df.to_csv(index=False)
        test_csv_data = test_df.to_csv(index=False)

        # Encode the CSV data to bytes
        train_csv_bytes = train_csv_data.encode('utf-8')
        test_csv_bytes = test_csv_data.encode('utf-8')

        # Get the current timestamp for unique file names
        current_timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

        # Define the names for the blobs
        train_blob_name = f"traindata_{current_timestamp}.csv"
        test_blob_name = f"testdata_{current_timestamp}.csv"

        # Define content settings for the blob
        content_settings = ContentSettings(content_type='application/octet-stream')

        # Get blob clients for the 'gold-level' container
        train_blob_client = destination_container_client.get_blob_client(blob=train_blob_name)
        test_blob_client = destination_container_client.get_blob_client(blob=test_blob_name)


        # Upload the train CSV data to the 'gold-level' container
        train_blob_client.upload_blob(train_csv_bytes, overwrite=True, content_settings=content_settings)

        # Upload the test CSV data to the 'gold-level' container
        test_blob_client.upload_blob(test_csv_bytes, overwrite=True, content_settings=content_settings)

        # Confirm upload
        return func.HttpResponse(
            f"Train and test data processed and uploaded to gold-level container successfully. "
            f"Train data: {train_blob_name}, Test data: {test_blob_name}.",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)

# Define your file-specific processing functions here

def process_bmx_file(year,data):
    # Apply transformations for BMX file
    return data[['SEQN','BMXBMI','BMXLEG','BMXWAIST','BMXWT','BMXHT','BMXARMC','BMXARML']]

def process_dbq_file(year,data):
    # Apply transformations for DBQ file
    if year in ['1999-2000', '2001-2002', '2003-2004']:
        data = data[['SEQN','DBD090']]
        data.rename(columns={'DBD090':'DBD895'}, inplace=True)
        data['DBD910'] = None
        data['DBD905'] = None
    elif year in ['2005-2006']:
        data = data[['SEQN','DBD091']]
        data.rename(columns={'DBD091':'DBD895'}, inplace=True)
        data['DBD910'] = None
        data['DBD905'] = None
    else:
        data = data[['SEQN','DBD895','DBD905','DBD910']]
    return data



def process_demo_file(year,data):
    # Apply transformations for DEMO file
    if year in ['1999-2000', '2001-2002']:
        data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','RIDRETH1','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR']]
        #adding columns FIALANG,AIALANGA ,SIALANG,MIALANG with value null
        data['FIALANG'] = None
        data['AIALANG'] = None
        data['SIALANG'] = None
        data['MIALANG'] = None

    elif year in ['2003-2004','2005-2006']:
        data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','RIDRETH1','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR','FIALANG','AIALANG','SIALANG','MIALANG']]
    
    elif year in ['2007-2008','2009-2010']:
         data.rename(columns={'DMDBORN2':'DMDBORN'}, inplace=True)
         data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','RIDRETH1','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR','FIALANG','AIALANG','SIALANG','MIALANG']]
   
    elif year in ['2011-2012','2013-2014','2015-2016']:
        #rename DMDBORN4 to DMDBORN
        data.rename(columns={'DMDBORN4':'DMDBORN'}, inplace=True)
        #rename AIALANGA to AIALANG
        data.rename(columns={'AIALANGA':'AIALANG'}, inplace=True)
        data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','RIDRETH1','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR','FIALANG','AIALANG','SIALANG','MIALANG']]
    else:
        #rename DMDBORN4 to DMDBORN
        data.rename(columns={'DMDBORN4':'DMDBORN'}, inplace=True)
        #rename AIALANGA to AIALANG
        data.rename(columns={'AIALANGA':'AIALANG'}, inplace=True)
        #rename DMDMARTZ to DMDMARTL
        data.rename(columns={'DMDMARTZ':'DMDMARTL'}, inplace=True)
        data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','RIDRETH1','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR','FIALANG','AIALANG','SIALANG','MIALANG']]
    return data

def process_ohq_file(year,data):
    # Apply transformations for OHQ file
    data = data[['SEQN','OHQ033']]
    return data

def process_slq_file(year,data):
    # Apply transformations for SLQ file
    if year in ['1999-2000', '2001-2002', '2003-2004','2015-2016','2017-2020']:        
        data = data[['SEQN','SLD012']]
    else: 
        #rename SLD010H to SLD012
        data.rename(columns={'SLD010H':'SLD012'}, inplace=True)
        data = data[['SEQN','SLD012']]
    return data

def process_smq_file(year,data):
    return data[['SEQN', 'SMQ020']]

def process_smqfam_file(year,data):
    if year in ['1999-2000','2001-2002','2003-2004','2005-2006','2007-2008','2009-2010','2011-2012']:
        #rename SMD415 to SMD460
        data.rename(columns={'SMD415':'SMD460'}, inplace=True)
        data = data[['SEQN','SMD460']]
    else:
        data = data[['SEQN','SMD460']]
    return data


def process_smqrtu_file(year,data):
    if year in ['2005-2006','2007-2008','2009-2010','2011-2012']:
        #rename SMQ690D to SMQ851
        data.rename(columns={'SMQ690D':'SMQ851'}, inplace=True)
        #rename SMQ680 to SMDANY
        data.rename(columns={'SMQ680':'SMDANY'}, inplace=True)
        #duplicate SMDANY and rename this new column to SMQ681
        data['SMQ681'] = data['SMDANY']
        data = data[['SEQN','SMQ851','SMDANY','SMQ681']]
    else:
        data = data[['SEQN','SMQ851','SMDANY','SMQ681']]
    return data

def process_smqmec_file(year,data):
    if year in ['1999-2000']:
        #rename SMD690D to SMQ851
        data.rename(columns={'SMD690D':'SMQ851'}, inplace=True)
        #rename SMD680 to SMDANY
        data.rename(columns={'SMD680':'SMDANY'}, inplace=True)
        #duplicate SMDANY and rename this new column to SMQ681
        data['SMQ681'] = data['SMDANY']
        data = data[['SEQN','SMQ851','SMDANY','SMQ681']]
    elif year in ['2001-2002','2003-2004']:
        #rename SMD690D to SMQ851
        data.rename(columns={'SMQ690D':'SMQ851'}, inplace=True)
        #rename SMD680 to SMDANY
        data.rename(columns={'SMQ680':'SMDANY'}, inplace=True)
        #duplicate SMDANY and rename this new column to SMQ681
        data['SMQ681'] = data['SMDANY']
        data = data[['SEQN','SMQ851','SMDANY','SMQ681']]

    return data

def process_whq_file(year,data):
    if year in ['1999-2000']:
        #rename WHD150 to WHQ150
        data.rename(columns={'WHD150':'WHQ150'}, inplace=True)
        data = data[['SEQN','WHQ150','WHD140','WHD050','WHD020','WHD010']]
    else:
        data = data[['SEQN','WHQ150','WHD140','WHD050','WHD020','WHD010']]
    return data

def process_cot_file(year,data):
    return data[['SEQN', 'LBXCOT']]         




