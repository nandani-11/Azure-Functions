import azure.functions as func
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Connection string to your Azure Storage account
        connection_string = os.environ['AzureWebJobsStorage']
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        source_container_client = blob_service_client.get_container_client("silver-level")
        destination_container_client = blob_service_client.get_container_client("gold-level")

        final_df = pd.DataFrame()  # Final dataframe to hold all the data

        # List directories (which are by year)
        years = ['1999-2000', '2001-2002', '2003-2004','2005-2006','2007-2008','2009-2010','2011-2012','2013-2014','2015-2016','2017-2020']  # List all years here
        patterns = ['BMX', 'DBQ', 'DEMO', 'OHQ', 'SLQ', 'SMQ', 'SMQFAM', 'SMQMEC', 'WHQ','COT','SMQRTU']
        
        for year in years:
            year_df = pd.DataFrame()  # DataFrame to hold year-specific data

            # List files in the year directory
            file_blobs = source_container_client.list_blobs(name_starts_with=f'{year}/')
            for blob in file_blobs:
                file_name = blob.name.split('/')[1]
                # Check if the file name contains any of the patterns
                if any(pattern in file_name for pattern in patterns):
                    # Remove underscores and any characters after it
                    clean_file_name = file_name.split('_')[0]
                    blob_client = source_container_client.get_blob_client(blob)
                    blob_data = blob_client.download_blob().readall()
                    data = pd.read_csv(BytesIO(blob_data))

                    # Add the year as a column before processing
                    data['Year'] = year

                    # Apply specific transformations based on the file pattern
                    if 'BMX' in clean_file_name:
                        data = process_bmx_file(year,data)
                    elif 'DBQ' in clean_file_name:
                        data = process_dbq_file(year,data)
                    elif 'DEMO' in clean_file_name:
                        data = process_demo_file(data)
                    elif 'OHQ' in clean_file_name:
                        data = process_ohq_file(data)
                    elif 'SLQ' in clean_file_name:
                        data = process_slq_file(data)
                    elif 'SMQ' in clean_file_name:
                        data = process_smq_file(data)
                    elif 'SMQFAM' in clean_file_name:
                        data = process_smqfam_file(data)
                    elif 'SMQMEC' in clean_file_name:
                        data = process_smqmec_file(data)
                    elif 'WHQ' in clean_file_name:
                        data = process_whq_file(data)
                    elif 'COT' in clean_file_name:
                        data = process_cot_file(data)
                    elif 'SMQRTU' in clean_file_name:
                        data = process_smqrtu_file(data)
                    else:
                        raise ValueError(f"File name {file_name} does not match any of the patterns.")
                    
                    # Merge the data from this file to the year-specific DataFrame
                    year_df = year_df.merge(data, on='SEQN', how='outer') if not year_df.empty else data

            # Add the year-specific data to the final DataFrame
            final_df = pd.concat([final_df, year_df], axis=0)

        # Upload the final DataFrame to the 'gold-level' container
        # Convert DataFrame to CSV
            csv_data = final_df.to_csv(index=False)

            # Define the name for the blob using the current timestamp or other unique identifier
            from datetime import datetime
            blob_name = f"combined_data_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv"

            # Define content settings for the blob
            content_settings = ContentSettings(content_type='text/csv')

            # Get a blob client for the 'gold-level' container
            blob_client = destination_container_client.get_blob_client(blob=blob_name)

            # Upload the CSV data to the 'gold-level' container
            blob_client.upload_blob(csv_data, overwrite=True, content_settings=content_settings)

        # Confirm upload
        return func.HttpResponse(f"Data processed and uploaded to gold-level/{blob_name} successfully.", status_code=200)

        # return func.HttpResponse(f"Data processed and stored successfully.", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)

# Define your file-specific processing functions here
# process_bmx_file, process_dbq_file, etc.
def process_bmx_file(year,data):
    # Apply transformations for BMX file
    return data[['SEQN','BMXBMI','BMXLEG','BMXWAIST','BMXWT','BMXHT','BMXARMC','BMXARML']]

def process_dbq_file(year,data):
    # Apply transformations for DBQ file
    if year in ['1999-2000', '2001-2002', '2003-2004','2005-2006']:
        data = data[['SEQN','DBD090']]
        data.rename(columns={'DBD090':'DBD895'}, inplace=True)
        data['DBD910'] = None
        data['DBD905'] = None
    else:
        data = data[['SEQN','DBD895','DBD905','DBD910']]
    return data

def process_demo_file(year,data):
    # Apply transformations for DEMO file
    if year in ['1999-2000', '2001-2002']:
        data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR']]
        #adding columns FIALANG,AIALANGA ,SIALANG,MIALANG with value null
        data['FIALANG'] = None
        data['AIALANG'] = None
        data['SIALANG'] = None
        data['MIALANG'] = None
    elif year in ['2003-2004','2005-2006']:
        data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR','FIALANG','AIALANG','SIALANG','MIALANG']]
    elif year in ['2007-2008','2009-2010']:
         data.rename(columns={'DMDBORN2':'DMDBORN'}, inplace=True)
         data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR','FIALANG','AIALANG','SIALANG','MIALANG']]
    elif year in ['2011-2012','2013-2014','2015-2016']:
        #rename DMDBORN4 to DMDBORN
        data.rename(columns={'DMDBORN4':'DMDBORN'}, inplace=True)
        #rename AIALANGA to AIALANG
        data.rename(columns={'AIALANGA':'AIALANG'}, inplace=True)
        data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR','FIALANG','AIALANG','SIALANG','MIALANG']]
    else:
        #rename DMDBORN4 to DMDBORN
        data.rename(columns={'DMDBORN4':'DMDBORN'}, inplace=True)
        #rename AIALANGA to AIALANG
        data.rename(columns={'AIALANGA':'AIALANG'}, inplace=True)
        #rename DMDMARTZ to DMDMARTL
        data.rename(columns={'DMDMARTZ':'DMDMARTL'}, inplace=True)
        data = data[['SEQN','RIAGENDR','RIDAGEYR','RIDEXMON','DMDBORN','DMDEDUC2','DMDMARTL','INDFMPIR','FIALANG','AIALANG','SIALANG','MIALANG']]
    


    return data


# ... rest of the code remains the same ...
